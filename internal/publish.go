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

	compose "github.com/compose-spec/compose-go/types"
	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/client"
)

func PinServiceImages(cli *client.Client, ctx context.Context, config *compose.Config) error {
	return config.WithServices(nil, func(service compose.ServiceConfig) error {
		fmt.Printf("Pinning %s(%s)\n", service.Name, service.Image)
		named, err := reference.ParseNormalizedNamed(service.Image)
		if err != nil {
			return err
		}

		di, err := cli.DistributionInspect(ctx, service.Image, "")
		if err != nil {
			fmt.Println("")
			return err
		}

		// TODO - we should find the intersection of platforms so
		// that we can denote the platforms this app can run on
		pinned := reference.Domain(named) + "/" + reference.Path(named) + "@" + di.Descriptor.Digest.String()
		fmt.Printf("  | ")
		for i, plat := range di.Platforms {
			if i != 0 {
				fmt.Printf(", ")
			}
			fmt.Printf(plat.Architecture)
			if plat.Architecture == "arm" {
				fmt.Printf(plat.Variant)
			}
		}
		fmt.Println("\n  |-> ", pinned)

		// Update the actual config and not our copy of it
		for i := range config.Services {
			if config.Services[i].Name == service.Name {
				config.Services[i].Image = pinned
				break
			}
		}
		return nil
	})
}

func createTgz(composeContent []byte, bundleDir string) ([]byte, error) {
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)

	tw := tar.NewWriter(gzw)

	err := filepath.Walk(bundleDir, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("Tar: Can't stat file %s to tar: %w", bundleDir, err)
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
		header.Name = strings.TrimPrefix(strings.Replace(file, bundleDir, "", -1), string(filepath.Separator))

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

func CreateBundle(ctx context.Context, config *compose.Config, target string) error {
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
	fmt.Println("  |-> bundle: ", desc.Digest.String())

	mb := ocischema.NewManifestBuilder(blobStore, []byte{}, map[string]string{"compose-bundle": "v1"})
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
