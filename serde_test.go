package conduit

import "testing"

func TestJSONRoundTrip(t *testing.T) {
	type payload struct { ID string `json:"id"` }
	serde := JSON[payload]{}
	data, err := serde.Serialize(payload{ID: "42"}); if err != nil { t.Fatal(err) }
	got, err := serde.Deserialize(data); if err != nil { t.Fatal(err) }
	if got.ID != "42" { t.Fatalf("ID = %q", got.ID) }
}

func TestBytesDoesNotAliasInput(t *testing.T) {
	input := []byte("hello")
	got, err := (Bytes{}).Deserialize(input); if err != nil { t.Fatal(err) }
	input[0] = 'j'
	if string(got) != "hello" { t.Fatal("deserialized bytes alias input") }
}
