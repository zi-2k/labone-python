import pytest

from labone.mock.convert_to_add_nodes import NUMBER_PLACEHOLDER, DynamicNestedStructure, BuildSegment, list_nodes_info_to_get_nodes



# @pytest.mark.parametrize(("listed_nodes"), [
#     ["/a"],
#     ["/a/b/c"],
#     ["/a/b", "/a/c"],
# ])
def test_conversion():
    listed_nodes = ["/a/b", "/a/c"]
    conversion = list_nodes_info_to_get_nodes({path:{} for path in listed_nodes})

    root = conversion[0]
    print({segment.id for segment in conversion})
    assert root.name == "a"
    assert {"a","b","c"} == {segment.name for segment in conversion}
    assert {subnode.name for subnode in root.subNodes} == {"b", "c"}

    conversion_dict = {segment.id:segment for segment in conversion}
    assert conversion_dict[root.subNodes[0].id].name in ["b","c"]
    assert len({segment.id for segment in conversion}) == len(conversion) # ids are unique


def test_conversion_range():
    listed_nodes = ["/a/0/b", "/a/1/b"]
    conversion = list_nodes_info_to_get_nodes({path:{} for path in listed_nodes})
    root = conversion[0]
    assert root.subNodes[0].name == NUMBER_PLACEHOLDER
    assert root.subNodes[0].range_end == 1


def test_conversion_multiple_starts():
    listed_nodes = ["/a", "/b"]
    conversion = list_nodes_info_to_get_nodes({path:{} for path in listed_nodes})


    root = conversion[0]
    assert root.subNodes[0].name == NUMBER_PLACEHOLDER
    assert root.subNodes[0].range_end == 1