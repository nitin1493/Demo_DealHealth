// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v3.21.5
// source: prediction.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	PredictionService_Predict_FullMethodName = "/PredictionService/Predict"
)

// PredictionServiceClient is the client API for PredictionService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type PredictionServiceClient interface {
	Predict(ctx context.Context, in *PredictionRequest, opts ...grpc.CallOption) (*PredictionResponse, error)
}

type predictionServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewPredictionServiceClient(cc grpc.ClientConnInterface) PredictionServiceClient {
	return &predictionServiceClient{cc}
}

func (c *predictionServiceClient) Predict(ctx context.Context, in *PredictionRequest, opts ...grpc.CallOption) (*PredictionResponse, error) {
	out := new(PredictionResponse)
	err := c.cc.Invoke(ctx, PredictionService_Predict_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PredictionServiceServer is the server API for PredictionService service.
// All implementations must embed UnimplementedPredictionServiceServer
// for forward compatibility
type PredictionServiceServer interface {
	Predict(context.Context, *PredictionRequest) (*PredictionResponse, error)
	mustEmbedUnimplementedPredictionServiceServer()
}

// UnimplementedPredictionServiceServer must be embedded to have forward compatible implementations.
type UnimplementedPredictionServiceServer struct {
}

func (UnimplementedPredictionServiceServer) Predict(context.Context, *PredictionRequest) (*PredictionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Predict not implemented")
}
func (UnimplementedPredictionServiceServer) mustEmbedUnimplementedPredictionServiceServer() {}

// UnsafePredictionServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to PredictionServiceServer will
// result in compilation errors.
type UnsafePredictionServiceServer interface {
	mustEmbedUnimplementedPredictionServiceServer()
}

func RegisterPredictionServiceServer(s grpc.ServiceRegistrar, srv PredictionServiceServer) {
	s.RegisterService(&PredictionService_ServiceDesc, srv)
}

func _PredictionService_Predict_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PredictionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PredictionServiceServer).Predict(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: PredictionService_Predict_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PredictionServiceServer).Predict(ctx, req.(*PredictionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// PredictionService_ServiceDesc is the grpc.ServiceDesc for PredictionService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var PredictionService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "PredictionService",
	HandlerType: (*PredictionServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Predict",
			Handler:    _PredictionService_Predict_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "prediction.proto",
}
