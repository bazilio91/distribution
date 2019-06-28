package storage

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/docker/distribution"
	"path"
	"testing"

	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/docker/distribution/testutil"
	"github.com/opencontainers/go-digest"
)

func TestStatsCleanRegistry(t *testing.T) {
	ctx := context.Background()
	inmemoryDriver := inmemory.New()

	registry := createRegistry(t, inmemoryDriver)
	repo := makeRepository(t, registry, "palailogos")
	manifestService, _ := repo.Manifests(ctx)

	image1 := uploadRandomSchema1Image(t, repo)
	image2 := uploadRandomSchema1Image(t, repo)
	uploadRandomSchema2Image(t, repo)

	// construct manifestlist for fun.
	blobstatter := registry.BlobStatter()
	manifestList, err := testutil.MakeManifestList(blobstatter, []digest.Digest{
		image1.manifestDigest, image2.manifestDigest})
	if err != nil {
		t.Fatalf("Failed to make manifest list: %v", err)
	}

	_, err = manifestService.Put(ctx, manifestList)
	if err != nil {
		t.Fatalf("Failed to add manifest list: %v", err)
	}

	//before := allBlobs(t, registry)

	// Run Stats
	results, err := Stats(context.Background(), inmemoryDriver, registry, StatsOpts{})
	if err != nil {
		t.Fatalf("Failed stats: %v", err)
	}

	spew.Dump(results)

	//after := allBlobs(t, registry)
	//if len(before) != len(after) {
	//	t.Fatalf("Garbage collection affected storage: %d != %d", len(before), len(after))
	//}
}

func TestStatsUntaggedManifestIfTagNotFound(t *testing.T) {
	ctx := context.Background()
	inmemoryDriver := inmemory.New()

	registry := createRegistry(t, inmemoryDriver)
	repo := makeRepository(t, registry, "deletemanifests")
	manifestService, _ := repo.Manifests(ctx)

	// Create random layers
	randomLayers1, err := testutil.CreateRandomLayers(3)
	if err != nil {
		t.Fatalf("failed to make layers: %v", err)
	}

	randomLayers2, err := testutil.CreateRandomLayers(3)
	if err != nil {
		t.Fatalf("failed to make layers: %v", err)
	}

	// Upload all layers
	err = testutil.UploadBlobs(repo, randomLayers1)
	if err != nil {
		t.Fatalf("failed to upload layers: %v", err)
	}

	err = testutil.UploadBlobs(repo, randomLayers2)
	if err != nil {
		t.Fatalf("failed to upload layers: %v", err)
	}

	// Construct manifests
	manifest1, err := testutil.MakeSchema1Manifest(getKeys(randomLayers1))
	if err != nil {
		t.Fatalf("failed to make manifest: %v", err)
	}

	manifest2, err := testutil.MakeSchema1Manifest(getKeys(randomLayers2))
	if err != nil {
		t.Fatalf("failed to make manifest: %v", err)
	}

	_, err = manifestService.Put(ctx, manifest1)
	if err != nil {
		t.Fatalf("manifest upload failed: %v", err)
	}

	_, err = manifestService.Put(ctx, manifest2)
	if err != nil {
		t.Fatalf("manifest upload failed: %v", err)
	}

	manifestEnumerator, _ := manifestService.(distribution.ManifestEnumerator)
	manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
		repo.Tags(ctx).Tag(ctx, "test", distribution.Descriptor{Digest: dgst})
		return nil
	})

	//before1 := allBlobs(t, registry)
	//before2 := allManifests(t, manifestService)

	// run GC with dry-run (should not remove anything)
	results, err := Stats(context.Background(), inmemoryDriver, registry, StatsOpts{})
	if err != nil {
		t.Fatalf("Failed mark and sweep: %v", err)
	}

	results.Print()

	//if _, ok := results.Untagged["deletemanifests"]; !ok {
	//	t.Fatalf("Untagged map is empty")
	//}

	//afterDry1 := allBlobs(t, registry)
	//afterDry2 := allManifests(t, manifestService)
	//if len(before1) != len(afterDry1) {
	//	t.Fatalf("Garbage collection affected blobs storage: %d != %d", len(before1), len(afterDry1))
	//}
	//if len(before2) != len(afterDry2) {
	//	t.Fatalf("Garbage collection affected manifest storage: %d != %d", len(before2), len(afterDry2))
	//}
	//
	//// Run GC (removes everything because no manifests with tags exist)
	//err = MarkAndSweep(context.Background(), inmemoryDriver, registry, GCOpts{
	//	DryRun:         false,
	//	RemoveUntagged: true,
	//})
	//if err != nil {
	//	t.Fatalf("Failed mark and sweep: %v", err)
	//}
	//
	//after1 := allBlobs(t, registry)
	//after2 := allManifests(t, manifestService)
	//if len(before1) == len(after1) {
	//	t.Fatalf("Garbage collection affected blobs storage: %d == %d", len(before1), len(after1))
	//}
	//if len(before2) == len(after2) {
	//	t.Fatalf("Garbage collection affected manifest storage: %d == %d", len(before2), len(after2))
	//}
}

func TestStatsWithMissingManifests(t *testing.T) {
	ctx := context.Background()
	d := inmemory.New()

	registry := createRegistry(t, d)
	repo := makeRepository(t, registry, "testrepo")
	uploadRandomSchema1Image(t, repo)

	// Simulate a missing _manifests directory
	revPath, err := pathFor(manifestRevisionsPathSpec{"testrepo"})
	if err != nil {
		t.Fatal(err)
	}

	_manifestsPath := path.Dir(revPath)
	err = d.Delete(ctx, _manifestsPath)
	if err != nil {
		t.Fatal(err)
	}

	_, err = Stats(context.Background(), d, registry, StatsOpts{})
	if err != nil {
		t.Fatalf("Failed mark and sweep: %v", err)
	}
}

func TestStatsWithSharedLayer(t *testing.T) {
	ctx := context.Background()
	inmemoryDriver := inmemory.New()

	registry := createRegistry(t, inmemoryDriver)
	repo := makeRepository(t, registry, "tzimiskes")
	manifestService, _ := repo.Manifests(ctx)


	// Create random layers
	randomLayers1, err := testutil.CreateRandomLayers(3)
	if err != nil {
		t.Fatalf("failed to make layers: %v", err)
	}

	randomLayers2, err := testutil.CreateRandomLayers(3)
	if err != nil {
		t.Fatalf("failed to make layers: %v", err)
	}

	// Upload all layers
	err = testutil.UploadBlobs(repo, randomLayers1)
	if err != nil {
		t.Fatalf("failed to upload layers: %v", err)
	}

	err = testutil.UploadBlobs(repo, randomLayers2)
	if err != nil {
		t.Fatalf("failed to upload layers: %v", err)
	}

	// Construct manifests
	manifest1, err := testutil.MakeSchema1Manifest(getKeys(randomLayers1))
	if err != nil {
		t.Fatalf("failed to make manifest: %v", err)
	}

	sharedKey := getAnyKey(randomLayers1)
	manifest2, err := testutil.MakeSchema2Manifest(repo, append(getKeys(randomLayers2), sharedKey))
	if err != nil {
		t.Fatalf("failed to make manifest: %v", err)
	}

		// Upload manifests
	_, err = manifestService.Put(ctx, manifest1)
	if err != nil {
		t.Fatalf("manifest upload failed: %v", err)
	}

	manifestDigest2, err := manifestService.Put(ctx, manifest2)
	if err != nil {
		t.Fatalf("manifest upload failed: %v", err)
	}

	// delete
	err = manifestService.Delete(ctx, manifestDigest2)
	if err != nil {
		t.Fatalf("manifest deletion failed: %v", err)
	}

	results, err := Stats(context.Background(), inmemoryDriver, registry, StatsOpts{})
	if err != nil {
		t.Fatalf("Failed stats: %v", err)
	}

	// len 0 of Untagged
	results.Print()
}

func TestStatsOrphanBlob(t *testing.T) {
	inmemoryDriver := inmemory.New()

	registry := createRegistry(t, inmemoryDriver)
	repo := makeRepository(t, registry, "michael_z_doukas")

	digests, err := testutil.CreateRandomLayers(1)
	if err != nil {
		t.Fatalf("Failed to create random digest: %v", err)
	}

	if err = testutil.UploadBlobs(repo, digests); err != nil {
		t.Fatalf("Failed to upload blob: %v", err)
	}

	// formality to create the necessary directories
	uploadRandomSchema2Image(t, repo)

	// Run Stats
	results, err := Stats(context.Background(), inmemoryDriver, registry, StatsOpts{})
	if err != nil {
		t.Fatalf("Failed mark and sweep: %v", err)
	}

	spew.Dump(results)

	//blobs := allBlobs(t, registry)
	//
	//// check that orphan blob layers are not still around
	//for dgst := range digests {
	//	if _, ok := blobs[dgst]; ok {
	//		t.Fatalf("Orphan layer is present: %v", dgst)
	//	}
	//}
}
