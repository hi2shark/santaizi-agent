package proto

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestProtocolExposesOnlyTypedSantaiziServices(t *testing.T) {
	services := File_proto_santaizi_proto.Services()
	expected := []protoreflect.Name{
		"SantaiziService", "SantaiziTelemetryService", "SantaiziControlService",
		"SantaiziNATService", "SantaiziReplicationService", "SantaiziCollectorService",
	}
	if services.Len() != len(expected) {
		t.Fatalf("service count=%d, want %d", services.Len(), len(expected))
	}
	for _, name := range expected {
		if services.ByName(name) == nil {
			t.Fatalf("missing typed service %s", name)
		}
	}
	control := services.ByName(protoreflect.Name("SantaiziControlService"))
	if control == nil || control.Methods().Len() != 1 || control.Methods().Get(0).Name() != "Control" {
		t.Fatalf("control service methods=%v", control)
	}
	nat := services.ByName(protoreflect.Name("SantaiziNATService"))
	if nat == nil || nat.Methods().Len() != 1 || nat.Methods().Get(0).Name() != "NATStream" {
		t.Fatalf("NAT service methods=%v", nat)
	}
}
