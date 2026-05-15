package xmeta

import "testing"

func TestMapMySQLTinyIntUsesColumnTypeForBoolean(t *testing.T) {
	if _, ok := mapMySQLTypeForProto("tinyint", "tinyint(1)", 0, 0, 0).TypeClause.(*DataType_BooleanData); !ok {
		t.Fatal("tinyint(1) should map to boolean")
	}
	tiny := mapMySQLTypeForProto("tinyint", "tinyint unsigned", 0, 0, 0).GetTinyIntData()
	if tiny == nil {
		t.Fatal("tinyint unsigned should map to TinyInt")
	}
	if !tiny.IsUnsigned {
		t.Fatal("tinyint unsigned should preserve unsigned flag")
	}
}
