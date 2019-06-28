package storage

import (
	"context"
	"fmt"
	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

type StatsOpts struct {
}

type StatsResults struct {
	Untagged    map[distribution.Repository]map[*digest.Digest]bool
	OrphanBlobs []*digest.Digest
	Tags        map[distribution.Repository]map[string]*digest.Digest

	registry distribution.Namespace
}

func getOrphanDigestSize(registry distribution.Namespace, d *digest.Digest) int64 {
	descr, err := registry.BlobStatter().Stat(context.Background(), *d)
	if err != nil {
		return 0
	}

	return descr.Size
}

func (r *StatsResults) Print() {
	ctx := context.Background()
	fmt.Println("repos:")
	for repo, tags := range r.Tags {
		fmt.Printf("  %s:\n", repo.Named().String())

		fmt.Print("    tags:\n")
		for tag, d := range tags {
			if d == nil {
				fmt.Printf("      %s:%s@%s\n", repo.Named().String(), tag, "missing")
			} else {
				ms, _ := repo.Manifests(ctx)
				m, err := ms.Get(ctx, *d)
				if err != nil {
					fmt.Println(err.Error())
				}

				size := int64(0)

				for _, ref := range m.References() {
					size += ref.Size
				}

				if err != nil {
					fmt.Printf("      %s:%s@%s\n", repo.Named().String(), tag, d.String())
				} else {
					fmt.Printf("      %s:%s@%s, size: %v\n", repo.Named().String(), tag, d.String(), byteCountBinary(size))
				}
			}
		}

		fmt.Print("    untagged:\n")
		for d, untagged := range r.Untagged[repo] {
			if !untagged {
				continue
			}

			ms, _ := repo.Manifests(ctx)
			m, err := ms.Get(ctx, *d)
			if err != nil {
				fmt.Println(err.Error())
			}

			size := int64(0)

			for _, ref := range m.References() {
				size += ref.Size
			}

			if err == nil {
				fmt.Printf("      %s@%s, size: %v\n", repo.Named().String(), d.String(), byteCountBinary(size))
			} else {
				fmt.Printf("      %s@%s\n", repo.Named().String(), d.String())
			}

		}
	}

	fmt.Println("orphan:")
	for _, d := range r.OrphanBlobs {
		fmt.Printf("  %s, size: %s\n", d.String(), byteCountBinary(getOrphanDigestSize(r.registry, d)))
	}
}

// MarkAndSweep performs a mark and sweep of registry data
func Stats(ctx context.Context, storageDriver driver.StorageDriver, registry distribution.Namespace, opts StatsOpts) (*StatsResults, error) {
	repositoryEnumerator, ok := registry.(distribution.RepositoryEnumerator)
	if !ok {
		return nil, fmt.Errorf("unable to convert Namespace to RepositoryEnumerator")
	}

	results := StatsResults{
		registry: registry,
	}
	results.Untagged = make(map[distribution.Repository]map[*digest.Digest]bool)
	results.Tags = make(map[distribution.Repository]map[string]*digest.Digest)

	markSet := make(map[digest.Digest]struct{})
	err := repositoryEnumerator.Enumerate(ctx, func(repoName string) error {
		var err error
		named, err := reference.WithName(repoName)
		if err != nil {
			return fmt.Errorf("failed to parse repo name %s: %v", repoName, err)
		}
		repository, err := registry.Repository(ctx, named)
		if err != nil {
			return fmt.Errorf("failed to construct repository: %v", err)
		}

		results.Untagged[repository] = map[*digest.Digest]bool{}
		results.Tags[repository] = map[string]*digest.Digest{}

		manifestService, err := repository.Manifests(ctx)
		if err != nil {
			return fmt.Errorf("failed to construct manifest service: %v", err)
		}

		manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
		if !ok {
			return fmt.Errorf("unable to convert ManifestService into ManifestEnumerator")
		}

		tagsService := repository.Tags(ctx)
		tags, err := tagsService.All(ctx)

		for _, value := range tags {
			results.Tags[repository][value] = nil
		}

		err = manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
			// fetch all tags where this manifest is the latest one
			tags, err := repository.Tags(ctx).Lookup(ctx, distribution.Descriptor{Digest: dgst})
			if err != nil {
				return fmt.Errorf("failed to retrieve tags for digest %v: %v", dgst, err)
			}
			if len(tags) == 0 {
				// fetch all tags from repository
				// all of these tags could contain manifest in history
				// which means that we need check (and delete) those references when deleting manifest

				results.Untagged[repository][&dgst] = true

				return nil
			} else {
				results.Untagged[repository][&dgst] = false
				markSet[dgst] = struct{}{}
				for _, tag := range tags {
					results.Tags[repository][tag] = &dgst
				}
			}

			return nil
		})

		if err != nil {
			// In certain situations such as unfinished uploads, deleting all
			// tags in S3 or removing the _manifests folder manually, this
			// error may be of type PathNotFound.
			//
			// In these cases we can continue marking other manifests safely.
			if _, ok := err.(driver.PathNotFoundError); ok {
				return nil
			}
		}

		for dgst, untagged := range results.Untagged[repository] {
			if untagged {
				continue
			}
			// Mark the manifest's blob
			manifest, err := manifestService.Get(ctx, *dgst)
			if err != nil {
				return fmt.Errorf("failed to retrieve manifest for digest %v: %v", dgst, err)
			}

			descriptors := manifest.References()
			for _, descriptor := range descriptors {
				markSet[descriptor.Digest] = struct{}{}
			}

		}

		return err
	})

	if err != nil {
		return nil, fmt.Errorf("failed to mark: %v", err)
	}

	results.OrphanBlobs = make([]*digest.Digest, 0)
	blobService := registry.Blobs()

	err = blobService.Enumerate(ctx, func(dgst digest.Digest) error {
		// check if digest is in markSet. If not, delete it!
		if _, ok := markSet[dgst]; !ok {
			results.OrphanBlobs = append(results.OrphanBlobs, &dgst)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error enumerating blobs: %v", err)
	}

	return &results, err
}

func byteCountBinary(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
