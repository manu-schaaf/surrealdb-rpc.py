from surrealdb_rpc.data_model.record_id import RecordId
from surrealdb_rpc.data_model.table import Table
from surrealdb_rpc.data_model.thing import Thing


class TestThing:
    def test_text(self):
        assert Thing("test", "foo").__surql__() == "test:foo"

        assert Thing("test", "foo-bar").__surql__() == "test:⟨foo-bar⟩"
        assert Thing("test", "foo bar").__surql__() == "test:⟨foo bar⟩"

        assert Thing("test", "42").__surql__() == "test:⟨42⟩"

        assert Thing.parse("test:1.0").__surql__() == "test:⟨1.0⟩"

        assert Thing("test", '{"foo": "bar"}').__surql__() == 'test:⟨{"foo": "bar"}⟩'

    def test_numeric(self):
        assert Thing("test", 42).__surql__() == "test:42"

    def test_array(self):
        assert Thing("test", ["foo", "bar"]).__surql__() == "test:['foo', 'bar']"

    def test_object(self):
        assert Thing("test", {"foo": "bar"}).__surql__() == "test:{ foo: 'bar' }"

    def test_parse(self):
        assert Thing("test", "foo") == Thing.parse("test:foo")
        assert Thing("test", "foo-bar") == Thing.parse("test:foo-bar")
        assert Thing("test", "foo bar") == Thing.parse("test:foo bar")
        assert Thing("test", "1.0") == Thing.parse("test:1.0")

        assert Thing("test", 42) == Thing.parse("test:42")


class TestRecordId:
    def test_text(self):
        assert RecordId("foo").__surql__() == "foo"
        assert RecordId.new("foo").__surql__() == "foo"

        assert RecordId("foo-bar").__surql__() == "⟨foo-bar⟩"
        assert RecordId("foo bar").__surql__() == "⟨foo bar⟩"

        assert RecordId("42").__surql__() == "⟨42⟩"

    def test_numeric(self):
        assert RecordId(42).__surql__() == "42"
        assert RecordId.new(42).__surql__() == "42"
        assert RecordId.new("42").__surql__() == "⟨42⟩"

        assert RecordId.parse("42").__surql__() == "42"
        assert RecordId.parse("⟨42⟩").__surql__() == "⟨42⟩"

    def test_array(self):
        assert RecordId(["foo", "bar"]).__surql__() == "['foo', 'bar']"
        assert RecordId.new(["foo", "bar"]).__surql__() == "['foo', 'bar']"

    def test_object(self):
        assert RecordId({"foo": "bar"}).__surql__() == "{ foo: 'bar' }"
        assert RecordId.new({"foo": "bar"}).__surql__() == "{ foo: 'bar' }"

    def test_object_nested(self):
        assert (
            RecordId.new({"foo": {"bar": "baz"}}).__surql__()
            == "{ foo: { bar: 'baz' } }"
        )

    def test_from_surql(self):
        assert RecordId.from_surql("⟨foo⟩").__surql__() == "⟨foo⟩"
        assert RecordId.from_surql("⟨foo:bar⟩").__surql__() == "⟨foo:bar⟩"
        assert RecordId.from_surql("⟨42⟩").__surql__() == "⟨42⟩"

        assert RecordId.from_surql("42").__surql__() == "42"

        assert RecordId.from_surql("['foo', 'bar']").__surql__() == "['foo', 'bar']"
        assert (
            RecordId.from_surql("['foo', { bar: 'baz' }]").__surql__()
            == "['foo', { bar: 'baz' }]"
        )

        assert RecordId.from_surql("{ foo: 'bar' }").__surql__() == "{ foo: 'bar' }"


class TestTable:
    def test_simple(self):
        table = Table("test")
        assert table.__surql__() == "test"

    def test_complex(self):
        table = Table("foo-bar")
        assert table.__surql__() == "⟨foo-bar⟩"

        table = Table("test:foo:bar")
        assert table.__surql__() == "⟨test:foo:bar⟩"
