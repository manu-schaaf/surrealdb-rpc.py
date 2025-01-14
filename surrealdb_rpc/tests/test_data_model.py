from surrealdb_rpc.data_model.record_id import RecordId
from surrealdb_rpc.data_model.table import Table
from surrealdb_rpc.data_model.thing import Thing


class TestThing:
    def test_text(self):
        assert Thing("test", "foo").__msgpack__() == "test:foo"

        assert Thing("test", "foo-bar").__msgpack__() == "test:⟨foo-bar⟩"
        assert Thing("test", "foo bar").__msgpack__() == "test:⟨foo bar⟩"

        assert Thing("test", "42").__msgpack__() == "test:⟨42⟩"

        assert Thing.from_str("test:1.0").__msgpack__() == "test:⟨1.0⟩"

    def test_numeric(self):
        assert Thing("test", 42).__msgpack__() == "test:42"

    def test_array(self):
        assert Thing("test", ["foo", "bar"]).__msgpack__() == "test:['foo', 'bar']"

    def test_object(self):
        assert Thing("test", {"foo": "bar"}).__msgpack__() == "test:{ foo: 'bar' }"

    def test_from_str(self):
        assert Thing("test", "foo") == Thing.from_str("test:foo")
        assert Thing("test", "foo-bar") == Thing.from_str("test:foo-bar")
        assert Thing("test", "foo bar") == Thing.from_str("test:foo bar")
        assert Thing("test", "1.0") == Thing.from_str("test:1.0")

        assert Thing("test", 42) == Thing.from_str("test:42")


class TestRecordId:
    def test_text(self):
        assert RecordId("foo").__msgpack__() == "foo"
        assert RecordId.new("foo").__msgpack__() == "foo"

        assert RecordId("foo-bar").__msgpack__() == "⟨foo-bar⟩"
        assert RecordId("foo bar").__msgpack__() == "⟨foo bar⟩"

        assert RecordId("42").__msgpack__() == "⟨42⟩"

    def test_numeric(self):
        assert RecordId(42).__msgpack__() == "42"
        assert RecordId.new(42).__msgpack__() == "42"
        assert RecordId.new("42").__msgpack__() == "42"

    def test_array(self):
        assert RecordId(["foo", "bar"]).__msgpack__() == "['foo', 'bar']"
        assert RecordId.new(["foo", "bar"]).__msgpack__() == "['foo', 'bar']"

    def test_object(self):
        assert RecordId({"foo": "bar"}).__msgpack__() == "{ foo: 'bar' }"
        assert RecordId.new({"foo": "bar"}).__msgpack__() == "{ foo: 'bar' }"

    def test_object_nested(self):
        assert (
            RecordId.new({"foo": {"bar": "baz"}}).__msgpack__()
            == "{ foo: { bar: 'baz' } }"
        )

    def test_from_raw(self):
        assert RecordId.from_raw("⟨foo⟩").__msgpack__() == "⟨foo⟩"
        assert RecordId.from_raw("⟨foo:bar⟩").__msgpack__() == "⟨foo:bar⟩"
        assert RecordId.from_raw("⟨42⟩").__msgpack__() == "⟨42⟩"

        assert RecordId.from_raw("42").__msgpack__() == "42"

        assert RecordId.from_raw("['foo', 'bar']").__msgpack__() == "['foo', 'bar']"
        assert (
            RecordId.from_raw("['foo', { bar: 'baz' }]").__msgpack__()
            == "['foo', { bar: 'baz' }]"
        )

        assert RecordId.from_raw("{ foo: 'bar' }").__msgpack__() == "{ foo: 'bar' }"


class TestTable:
    def test_simple(self):
        table = Table("test")
        assert table.__msgpack__() == "test"

    def test_complex(self):
        table = Table("foo-bar")
        assert table.__msgpack__() == "⟨foo-bar⟩"

        table = Table("test:foo:bar")
        assert table.__msgpack__() == "⟨test:foo:bar⟩"
