from typing import Any
from labone.node_info import NodeInfo
from labone.nodetree.helper import split_path

NUMBER_PLACEHOLDER = 'N'

class DynamicNestedStructure:
    def nest(self, name):
        sub = DynamicNestedStructure()
        self.__dict__[name] = sub
        return sub

class IdGenerator:
    def __init__(self) -> None:
        self.next_id = 0
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        self.next_id += 1
        return self.next_id

class BuildSegment:
    """Cannot deal with ranges yet. Does not use info yet."""
    def __init__(self, name, id) -> None:
        self.id = id
        self.name = name
        self.sub_segments = {} # name -> BuildSegment
        self.range_end = 0

    def add(self, segments, id_generator) -> None:
        next_segment = segments.pop(0)
        nr = 0
        if next_segment.isnumeric():
            nr = int(next_segment)
            next_segment = NUMBER_PLACEHOLDER

        if next_segment not in self.sub_segments:
            self.sub_segments[next_segment] = BuildSegment(next_segment, id_generator())

        self.sub_segments[next_segment].range_end = max(nr, self.sub_segments[next_segment].range_end)
        if segments:
            self.sub_segments[next_segment].add(segments, id_generator)

    def to_capnp(self):
        mock_capnp = DynamicNestedStructure()
        mock_capnp.id = self.id
        mock_capnp.name = self.name
        mock_capnp.rangeEnd = self.range_end
        mock_capnp.subNodes = [sub for name, sub in self.sub_segments.items()]

        info = mock_capnp.nest("info")
        info.description = "some description"
        info.properties = ['read', 'write', 'setting']
        info.type = "int64"
        info.unit = "None"

        result =  [mock_capnp] 
        for sub in self.sub_segments.values():
            result += sub.to_capnp()
        return result
        

def list_nodes_info_to_get_nodes(listed_nodes_info: dict[str, NodeInfo]):
    id_generator = IdGenerator()
    virtual_root = BuildSegment("virtual_root", id_generator())
    for path, info in listed_nodes_info.items():
        virtual_root.add(split_path(path), id_generator)

    result = []
    for sub_node in virtual_root.sub_segments.values():
        result += sub_node.to_capnp()
    return result