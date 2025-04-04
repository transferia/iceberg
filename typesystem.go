package iceberg

import (
	"github.com/apache/iceberg-go"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

func init() {
	typesystem.SourceRules(ProviderType, map[schema.Type][]string{
		schema.TypeInt64:     {new(iceberg.Int64Type).Type()},
		schema.TypeInt32:     {new(iceberg.Int32Type).Type()},
		schema.TypeInt16:     {},
		schema.TypeInt8:      {},
		schema.TypeUint64:    {},
		schema.TypeUint32:    {},
		schema.TypeUint16:    {},
		schema.TypeUint8:     {},
		schema.TypeFloat32:   {new(iceberg.Float32Type).Type()},
		schema.TypeFloat64:   {new(iceberg.Float64Type).Type()},
		schema.TypeBytes:     {new(iceberg.BinaryType).Type()},
		schema.TypeString:    {new(iceberg.StringType).Type(), new(iceberg.UUIDType).Type()},
		schema.TypeBoolean:   {new(iceberg.BooleanType).Type()},
		schema.TypeDate:      {new(iceberg.DateType).Type()},
		schema.TypeDatetime:  {},
		schema.TypeTimestamp: {new(iceberg.TimestampType).Type(), new(iceberg.TimestampTzType).Type()},
		schema.TypeInterval:  {},
		schema.TypeAny: {
			typesystem.RestPlaceholder,
		},
	})
	typesystem.TargetRule(ProviderType, map[schema.Type]string{
		schema.TypeInt64:     new(iceberg.Int64Type).Type(),
		schema.TypeInt32:     new(iceberg.Int32Type).Type(),
		schema.TypeInt16:     new(iceberg.Int32Type).Type(),
		schema.TypeInt8:      new(iceberg.Int32Type).Type(),
		schema.TypeUint64:    new(iceberg.Int64Type).Type(),
		schema.TypeUint32:    new(iceberg.Int64Type).Type(),
		schema.TypeUint16:    new(iceberg.Int32Type).Type(),
		schema.TypeUint8:     new(iceberg.Int32Type).Type(),
		schema.TypeFloat32:   new(iceberg.Float32Type).Type(),
		schema.TypeFloat64:   new(iceberg.Float64Type).Type(),
		schema.TypeBytes:     new(iceberg.BinaryType).Type(),
		schema.TypeString:    new(iceberg.StringType).Type(),
		schema.TypeBoolean:   new(iceberg.BooleanType).Type(),
		schema.TypeAny:       new(iceberg.StructType).Type(),
		schema.TypeDate:      new(iceberg.DateType).Type(),
		schema.TypeDatetime:  new(iceberg.TimestampTzType).Type(),
		schema.TypeTimestamp: new(iceberg.TimestampTzType).Type(),
	})
}
