package grpc_service

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	// video_pb "./grpc_service"
)

var (
	jobClientConfig = fmt.Sprintf(`
        {
	        "loadBalancingPolicy": "round_robin"
	    }`)
	kacp = keepalive.ClientParameters{
		Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
		Timeout:             1 * time.Second,  // wait 1 second for ping ack before considering the connection dead
		PermitWithoutStream: true,             // send pings even without active streams
	}
)

type W3StreamClient struct {
	addr string

	client GRPCServiceClient
	conn   *grpc.ClientConn
}

func NewW3streamClient(addr string) (*W3StreamClient, error) {
	cb := GrpcConnBuilder{}
	cb.WithInsecure()
	cb.WithContext(context.Background())
	cb.WithOptions(
		grpc.WithDefaultServiceConfig(jobClientConfig),
		grpc.WithKeepaliveParams(kacp),
	)

	conn, err := cb.GetConn(addr)
	if err != nil {
		return nil, err
	}

	return &W3StreamClient{
		addr: addr,

		client: NewGRPCServiceClient(conn),
		conn:   conn,
	}, nil
}

func (c *W3StreamClient) UploadVideo(
	ctx context.Context,
	videoId string,
	fileName string,
	size int64,
	reader io.Reader,
) error {
	stream, err := c.client.UploadVideo(ctx)
	if err != nil {
		return err
	}

	for {
		buf := make([]byte, 1024)
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		md5Hash := md5.Sum(buf[:n])
		if err := stream.Send(&UploadVideoRequest{
			VideoId: videoId,
			Data:  buf[:n],
			BlockMetadata: &BlockMetadata{
				Size:     int32(n),
				Checksum: fmt.Sprintf("%x", md5Hash),
			},
			FileInfo: &FileInfo{
				Name: fileName,
				Size: size,
				Type: "source",
			},
		}); err != nil {
			return err
		}
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return err
	}

	return nil
}
