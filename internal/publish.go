package internal

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/client"
)

func PinServiceImages(cli *client.Client, ctx context.Context, services map[string]interface{}) error {
	regc := NewRegistryClient()
	for name, obj := range services {
		svc, ok := obj.(map[string]interface{})
		if !ok {
			return fmt.Errorf("Service(%s) has invalid format", name)
		}
		obj, ok := svc["image"]
		if !ok {
			return fmt.Errorf("Service(%s) missing 'image' attribute", name)
		}
		image, ok := obj.(string)
		if !ok {
			return fmt.Errorf("Service(%s) invalid 'image' attribute", name)
		}
		fmt.Printf("Pinning %s(%s)\n", name, image)
		named, err := reference.ParseNormalizedNamed(image)
		if err != nil {
			return err
		}

		repo, err := regc.GetRepository(ctx, named)
		if err != nil {
			return err
		}
		tag := named.(reference.Tagged).Tag()
		desc, err := repo.Tags(ctx).Get(ctx, tag)
		mansvc, err := repo.Manifests(ctx, nil)
		man, err := mansvc.Get(ctx, desc.Digest)

		// TODO - we should find the intersection of platforms so
		// that we can denote the platforms this app can run on
		pinned := reference.Domain(named) + "/" + reference.Path(named) + "@" + desc.Digest.String()

		switch mani := man.(type) {
		case *manifestlist.DeserializedManifestList:
			fmt.Printf("  | ")
			for i, m := range mani.Manifests {
				if i != 0 {
					fmt.Printf(", ")
				}
				fmt.Printf(m.Platform.Architecture)
				if m.Platform.Architecture == "arm" {
					fmt.Printf(m.Platform.Variant)
				}
			}
		case *schema2.DeserializedManifest:
			break
		default:
			return fmt.Errorf("Unexpected manifest: %v", mani)
		}

		fmt.Println("\n  |-> ", pinned)
		svc["image"] = pinned
	}
	return nil
}

func createTgz(composeContent []byte, appDir string) ([]byte, error) {
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)

	tw := tar.NewWriter(gzw)

	err := filepath.Walk(appDir, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("Tar: Can't stat file %s to tar: %w", appDir, err)
		}

		if fi.Mode().IsDir() {
			return nil
		}
		if !fi.Mode().IsRegular() {
			// TODO handle the different types similar to
			// https://github.com/moby/moby/blob/master/pkg/archive/archive.go#L573
			return fmt.Errorf("Tar: Can't tar non regular types yet: %s", fi.Name())
		}
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}
		if fi.Name() == "docker-compose.yml" {
			header.Size = int64(len(composeContent))
		}

		// Handle subdirectories
		header.Name = strings.TrimPrefix(strings.Replace(file, appDir, "", -1), string(filepath.Separator))

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if fi.Name() == "docker-compose.yml" {
			tw.Write(composeContent)
		} else {
			f, err := os.Open(file)
			if err != nil {
				f.Close()
				return err
			}
			if _, err := io.Copy(tw, f); err != nil {
				return err
			}
			f.Close()
		}

		return nil
	})

	tw.Close()
	gzw.Close()

	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func CreateApp(ctx context.Context, config map[string]interface{}, target string) error {
	pinned, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	buff, err := createTgz(pinned, "./")
	if err != nil {
		return err
	}

	named, err := reference.ParseNormalizedNamed(target)
	if err != nil {
		return err
	}
	tag := "latest"
	if tagged, ok := reference.TagNameOnly(named).(reference.Tagged); ok {
		tag = tagged.Tag()
	}

	regc := NewRegistryClient()
	repo, err := regc.GetRepository(ctx, named)
	if err != nil {
		return err
	}

	blobStore := repo.Blobs(ctx)
	desc, err := blobStore.Put(ctx, "application/tar+gzip", buff)
	if err != nil {
		return nil
	}
	fmt.Println("  |-> app: ", desc.Digest.String())

	mb := ocischema.NewManifestBuilder(blobStore, []byte{}, map[string]string{"compose-app": "v1"})
	mb.AppendReference(desc)

	manifest, err := mb.Build(ctx)
	if err != nil {
		return err
	}
	svc, err := repo.Manifests(ctx, nil)
	if err != nil {
		return err
	}

	putOptions := []distribution.ManifestServiceOption{distribution.WithTag(tag)}
	digest, err := svc.Put(ctx, manifest, putOptions...)
	if err != nil {
		return err
	}
	fmt.Println("  |-> manifest: ", digest.String())

	return err
}
