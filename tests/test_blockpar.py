import pytest

from rangers.blockpar import BlockPar, BlockParPathError, BlockParFormatError
from io import StringIO


@pytest.fixture(params=[True, False])
def bp(request: pytest.FixtureRequest):
    return BlockPar(sort=request.param)


# Simple addition and retrieval
def test_add_and_get_param(bp: BlockPar):
    bp.add("param", "value")
    assert bp.get("param") == "value"
    assert bp.getone("param") == "value"
    assert bp.getall("param") == ["value"]


# Value replacement
def test_set_param(bp: BlockPar):
    bp.add("p", "1")
    bp.set("p", "2")
    assert bp.getone("p") == "2"
    assert bp.getall("p") == ["2"]


# Deletion by key and index
def test_delete_key_and_index(bp: BlockPar):
    bp.add("x", "1")
    bp.add("x", "2")
    bp.delete("x", -1)
    assert bp.getone("x") == "1"
    bp.delete("x")
    assert "x" not in bp


# Index out of range
def test_index_out_of_bounds(bp: BlockPar):
    bp.add("key", "val")
    with pytest.raises(IndexError):
        bp.getone("key", 2)


# Missing key
def test_missing_key(bp: BlockPar):
    with pytest.raises(KeyError):
        bp.getone("missing")


# Nesting and path access
def test_nested_block_access(bp: BlockPar):
    child = BlockPar()
    child.add("param", "123")
    bp.add("sub", child)

    assert bp.get_block("sub") == child
    assert bp.get_par("sub.param") == "123"


# Errors on invalid path
def test_invalid_path_errors(bp: BlockPar):
    with pytest.raises(BlockParPathError):
        bp.get_par("nonexistent.value")
    with pytest.raises(BlockParPathError):
        bp.get_block("bad.block")


# Save and load to/from text
def test_txt_save_load(bp: BlockPar):
    bp.add("x", "line1\nline2")
    s = StringIO()
    bp.save_txt(s)
    text = s.getvalue()

    # Reload
    s2 = StringIO(text)
    new_bp = BlockPar()
    new_bp.load_txt(s2)

    content = new_bp.getone("x")
    assert content == "line1\nline2"


# Invalid heredoc format
def test_bad_heredoc_format():
    s = StringIO("x=<<<\nline1\nline2")  # no >>> marker
    bp = BlockPar()
    with pytest.raises(BlockParFormatError):
        bp.load_txt(s)


# Iteration test
def test_iter_blockpar(bp: BlockPar):
    bp = BlockPar()
    bp.add("a", "1")
    bp.add("b", "2")
    bp.add("a", "3")
    items = [it for it in bp]
    assert items == [("a", "1"), ("a", "3"), ("b", "2")]


# Testing __contains__ and __len__
def test_contains_and_len(bp: BlockPar):
    bp = BlockPar()
    assert "k" not in bp
    bp.add("k", "val")
    assert "k" in bp
    assert len(bp) == 1
    bp.delete("k")
    assert "k" not in bp
    assert len(bp) == 0
