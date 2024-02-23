"""LabOne object-based node-tree implementation.

This module contains the core functionality of the node-tree. It provides
the classes for the different types of nodes, the node info and the
NodeTreeManager, which is the interface to the server.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import typing as t
import warnings
from abc import ABC, abstractmethod
from functools import cached_property

from deprecation import deprecated

from labone.core.subscription import DataQueue
from labone.core.value import AnnotatedValue, Value
from labone.mock.convert_to_add_nodes import DynamicNestedStructure
from labone.node_info import NodeInfo
from labone.nodetree.errors import (
    LabOneInappropriateNodeTypeError,
    LabOneInvalidPathError,
    LabOneNotImplementedError,
)
from labone.nodetree.helper import (
    NUMBER_PLACEHOLDER,
    WILDCARD,
    NestedDict,
    NormalizedPathSegment,
    Session,
    TreeProp,
    UndefinedStructure,
    build_prefix_dict,
    join_path,
    normalize_path_segment,
    pythonify_path_segment,
    split_path,
)

if t.TYPE_CHECKING:
    from labone.core.helper import LabOneNodePath
    from labone.core.session import NodeInfo as NodeInfoType
    from labone.core.subscription import QueueProtocol
    from labone.nodetree.enum import NodeEnum

T = t.TypeVar("T")


@dataclass
class TreeData:
    session: Session
    id_to_segment: dict[int, t.Any]
    root_id: str
    hide_kernel_prefix: bool
    parser: t.Callable[[AnnotatedValue], AnnotatedValue]

    def get_root(self, hide_kernel_prefix) -> Node:
        capnp_struct=self.id_to_segment[self.root_id]

        if hide_kernel_prefix:
            return Node.build(
            tree_data=self,
            capnp_struct=capnp_struct,
            parametrization=[],
            abstract_path_segments=(capnp_struct.name,),
            is_wildcard=False,
            )

        abstract_capnp = DynamicNestedStructure()
        abstract_capnp.name = ""
        capnp_subnode = DynamicNestedStructure()
        capnp_subnode.name = capnp_struct.name
        capnp_subnode.id = capnp_struct.id
        abstract_capnp.subNodes = [capnp_subnode]

        return Node.build(
            tree_data=self, 
            capnp_struct=abstract_capnp,
            parametrization=[],
            abstract_path_segments=(),
            is_wildcard=False,
            )
    
    @property
    def root(self):
        return self.get_root(hide_kernel_prefix=self.hide_kernel_prefix)


class MetaNode(ABC):
    """Basic functionality of all nodes.

    This class provides common behavior for all node classes, both normal nodes
    and result nodes. This includes the traversal of the tree and the generation
    of sub-nodes.

    Args:
        tree_manager: Interface managing the node-tree and the corresponding
            session.
        path_segments: A tuple describing the path.
        subtree_paths: Structure, defining which sub-nodes exist.
            May contain a Nested dictionary or a list of paths. If a list is
            passed, a prefix-to-suffix-dictionary will be created out of it.
    """

    def __init__(
        self,
        tree_data: TreeData,
        capnp_struct,
        parametrization: list[int],
        abstract_path_segments: tuple[NormalizedPathSegment, ...],
    ) -> None:
        self._tree_data = tree_data
        self.capnp_struct = capnp_struct
        self.parametrization = parametrization
        self._abstract_path_segments = abstract_path_segments

    @abstractmethod
    def __getattr__(self, next_segment: str) -> MetaNode | AnnotatedValue:
        """Access sub-node or value.

        ```python
        node.demods  # where 'demods' is the next segment to enter
        ```

        Args:
            next_segment: Segment, with which the current path should be extended

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node of a result.
        """
        ...

    @abstractmethod
    def __getitem__(self, path_extension: str | int) -> MetaNode | AnnotatedValue:
        """Go one or multiple levels deeper into the tree structure.

        The primary purpose of this operator is to allow accessing subnodes that
        are numbers (e.g. /0, /1, ...). The attribute access operator (dot) does
        not allow this, because numbers are not valid identifiers in Python.

        However, this operator can deal with a number of different scenarios:

        - simple path extensions:

            ```python
            node['deeper']
            ```

        - path indexing:

            ```python
            node['deeper/path']
            ```

        - numeric indexing:

            ```python
            node[0]
            ```

        - wildcards (placeholder for multiple path-extensions):

            ```python
            node['*']
            ```

        - combinations of all that:

            ```python
            node['deeper/*/path/0']
            ```


        All these implementations are equivalent:

        ```python
        node['mds/groups/0']
        node['mds']['groups'][0]
        node.mds.groups[0]
        ```

        Args:
            path_extension: path, number or wildcard.

        Returns: New node-object, representing the extended path, or plain value,
            if came to a leaf-node.
        """
        ...

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> Node:
        """Provides nodes for the extended path or the original values for leafs.

        Will fail if the resulting Path is ill-formed.

        Args:
            next_path_segment: Segment, with which the current path should be
            extended.

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.

        Raises:
            LabOneInvalidPathError: If the extension leads to an invalid path.

        """
        if next_path_segment == WILDCARD:
            return WildcardNode(
                tree_data=self.tree_data,
                capnp_struct=self.capnp_struct,
                parametrization=self.parametrization,
                abstract_path_segments=(*self.abstract_path_segments,WILDCARD),
            )
        
        if next_path_segment.isnumeric():
            nr = int(next_path_segment)
            if  nr < 0 and nr > sub_capnp.rangeEnd:
                msg = (
                    f"Path '{join_path((*self.path_segments, next_path_segment))}' is illegal, because '{next_path_segment}' "
                    f"is not a viable extension of '{join_path(self.path_segments)}'. "
                    f"Only the indeces between 0 and {sub_capnp.rangeEnd} exist."
                )
                raise LabOneInvalidPathError(msg) from e

            try:
                sub_id = self.segment_to_subnode[NUMBER_PLACEHOLDER]
            except KeyError as e:
                msg = (
                    f"Path '{join_path((*self.path_segments, next_path_segment))}' is illegal, because '{next_path_segment}' "
                    f"is not indexable. "
                    f"\nViable extensions would be {list(self.segment_to_subnode.keys())}"
                )
                raise LabOneInvalidPathError(msg) from e

            sub_capnp = self.tree_data.id_to_segment[sub_id]
            new_parametrization = self.parametrization.copy()
            new_parametrization.append(nr)
            return Node.build(
                tree_data=self.tree_data,
                capnp_struct=sub_capnp,
                parametrization=new_parametrization,
                abstract_path_segments=(*self.path_segments,sub_capnp.name),
                is_wildcard=False,
            )

        try:
            sub_id = self.segment_to_subnode[next_path_segment]
        except KeyError as e:
            msg = (
                f"Path '{join_path((*self.path_segments, next_path_segment))}' is illegal, because '{next_path_segment}' "
                f"is not a viable extension of '{join_path(self.path_segments)}'. "
                f"It does not correspond to any existing node."
                f"\nViable extensions would be {list(self.segment_to_subnode.keys())}"
            )
            raise LabOneInvalidPathError(msg) from e
        sub_capnp = self.tree_data.id_to_segment[sub_id]
        return Node.build(
            tree_data=self.tree_data,
            capnp_struct=sub_capnp,
            parametrization=self.parametrization.copy(),
            abstract_path_segments=(*self.path_segments,sub_capnp.name),
            is_wildcard=False,
        )

    def __iter__(self) -> t.Iterator[MetaNode | AnnotatedValue]:
        """Iterating through direct sub-nodes.

        The paths are traversed in a sorted manner, providing a clear order.
        This is particularly useful when iterating through numbered child nodes,
        such as /0, /1, ... or alphabetically sorted child nodes.

        Returns:
            Sub-nodes iterator.
        """
        for segment in sorted(self.segment_to_subnode.keys()):
            yield self[segment]

    def __len__(self) -> int:
        """Number of direct sub-nodes."""
        return len(self.segment_to_subnode)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self!s})"

    def __str__(self) -> str:
        return self.path

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, MetaNode):  # pragma: no cover
            msg = (
                f"'<' not supported between instances of "
                f"'{type(self)}' and '{type(other)}'"
            )  # pragma: no cover
            raise TypeError(msg)  # pragma: no cover
        return self.path < other.path

    @property
    def children(self) -> list[str]:
        """List of direct sub-node names."""
        return [pythonify_path_segment(p) for p in self.segment_to_subnode]

    def is_child_node(
        self,
        child_node: MetaNode | t.Sequence[NormalizedPathSegment],
    ) -> bool:
        """Checks if a node is a direct child node of this node.

        Children of children (etc.) will not be counted as direct children.
        The node itself is also not counted as its child.

        Args:
            child_node: Potential child node.

        Returns:
            Boolean if passed node is a child node.
        """
        path_segments = (
            child_node.path_segments
            if isinstance(child_node, MetaNode)
            else tuple(child_node)
        )

        return (
            len(path_segments) == len(self.path_segments) + 1
            and path_segments[: len(self.path_segments)] == self.path_segments
            and path_segments[-1] in self.segment_to_subnode
        )

    @property
    def tree_data(self) -> TreeData:
        """Get interface managing the node-tree and the corresponding session."""
        return self._tree_data
    
    @property#@cached_property
    def segment_to_subnode(self):
        return {e.name: e.id for e in self.capnp_struct.subNodes}
    
    @property
    def path(self) -> LabOneNodePath:
        return join_path(self.path_segments_generator)
    
    @property
    def path_segments(self) -> tuple[NormalizedPathSegment, ...]:
        return tuple(self.path_segments_generator)
    
    @property#@cached_property
    def path_segments_generator(self) -> t.Iterator[NormalizedPathSegment]:
        """Fill parametrization into the path segments."""
        parameter_index = 0
        for segment in self.abstract_path_segments:
            if segment == NUMBER_PLACEHOLDER:
                yield str(self.parametrization[parameter_index])
                parameter_index += 1
            else:
                yield segment

    @property
    def abstract_path_segments(self) -> tuple[NormalizedPathSegment, ...]:
        return self._abstract_path_segments
    
    @abstract_path_segments.setter
    def abstract_path_segments(self, value: tuple[NormalizedPathSegment, ...]) -> None:
        self._abstract_path_segments = value

    def __dir__(self) -> t.Iterable[str]:
        """Show valid subtree-extensions in hints.

        Returns:
            Iterator of valid dot-access identifier.

        """
        return self.children + list(super().__dir__())
    
    @property
    def root(self):
        """Providing root node.

        Depending on the hide_kelnel_prefix-setting of the
        NodeTreeManager, the root will either be '/' or
        the directly entered device, like '/dev1234'

        Returns:
            Root of the tree structure, this node is part of.
        """
        return self.tree_data.root


class ResultNode(MetaNode):
    """Representing values of a get-request in form of a tree.

    When issuing a get-request on a partial or wildcard node, the server will
    return a list of AnnotatedValues for every leaf-node in the subtree.
    This class adds the same object oriented interface as the normal nodes, when
    hitting a leaf node the result is returned.

    This allows to work with the results of a get-request in the same way as
    with the normal nodes. If needed one can still iterate through all the
    results. `results` will return an iterator over all results, not only the
    direct children.

    Args:
        tree_manager:
            Interface managing the node-tree and the corresponding session.
        path_segments: A tuple describing the path.
        subtree_paths:
            Structure, defining which sub-nodes exist.
        value_structure:
            Storage of the values at the leaf-nodes. They will be
            returned once the tree is traversed.
        timestamp:
            The time the results where created.
    """

    def __init__(  # noqa: PLR0913
        self,
        tree_data: TreeData,
        capnp_struct,
        parametrization: list[int],
        abstract_path_segments: tuple[NormalizedPathSegment, ...],
        value_structure: TreeProp,
        timestamp: int,
    ):
        super().__init__(
            tree_data=tree_data,
            capnp_struct=capnp_struct,
            parametrization=parametrization,
            abstract_path_segments=abstract_path_segments,
        )
        self._value_structure = value_structure
        self._timestamp = timestamp

    def __getattr__(self, next_segment: str) -> ResultNode | AnnotatedValue:
        """Access sub-node or value.

        Args:
            next_segment: Segment, with which the current path should be extended.

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.
        """
        return self.try_generate_subnode_result(normalize_path_segment(next_segment))

    def __getitem__(self, path_extension: str | int) -> ResultNode | AnnotatedValue:
        """Go one or multiple levels deeper into the tree structure.

        The primary purpose of this operator is to allow accessing subnodes that
        are numbers (e.g. /0, /1, ...). The attribute access operator (dot) does
        not allow this, because numbers are not valid identifiers in Python.

        However, this operator can deal with a number of different scenarios:

        - simple path extensions:
            >>> node['deeper']
        - path indexing:
            >>> node['deeper/path']
        - numeric indexing:
            >>> node[0]
        - wildcards (placeholder for multiple path-extensions):
            >>> node['*']
        - combinations of all that:
            >>> node['deeper/*/path/0']

        Args:
            path_extension: path, number or wildcard.

        Returns: New node-object, representing the extended path, or plain value,
            if came to a leaf-node.

        Raises:
            LabOneInvalidPathError: If path is invalid.

        """
        relative_path_segments = split_path(str(path_extension))
        current_node = self
        try:
            for path_segment in relative_path_segments:
                current_node = current_node.try_generate_subnode_result(
                    normalize_path_segment(path_segment),
                )  # type: ignore[assignment]

        except AttributeError as e:
            msg = (
                f"Path {join_path((*self.abstract_path_segments,*relative_path_segments))} "
                f"is invalid, because {current_node.path} "
                f"is already a leaf-node."
            )
            raise LabOneInvalidPathError(msg) from e

        return current_node

    def __contains__(self, item: str | int | ResultNode | AnnotatedValue) -> bool:
        """Checks if a path-segment or node is an immediate sub-node of this one."""
        if isinstance(item, ResultNode):
            return self.is_child_node(item)
        if isinstance(item, AnnotatedValue):
            return self.is_child_node(split_path(item.path))
        return normalize_path_segment(item) in self._subtree_paths

    def __call__(self, *_, **__) -> None:
        """Not supported on the Result node.

        Showing an error to express result-nodes can't be get/set.

        Raises:
            LabOneInappropriateNodeTypeError: Always.
        """
        msg = (
            "Trying to get/set a result node. This is not possible, because "
            "result nodes represents values of a former get-request. "
            "To interact with a device, make sure to operate on normal nodes."
        )
        raise LabOneInappropriateNodeTypeError(msg)

    def __str__(self) -> str:
        value_dict = {
            path: self._value_structure[path].value
            for path in self._value_structure
            if path.startswith(self.path)
        }
        return (
            f"{self.__class__.__name__}('{self.path}', time: #{self._timestamp}, "
            f"data: {value_dict})"
        )

    def __repr__(self) -> str:
        return f"{self!s}"

    def results(self) -> t.Iterator[AnnotatedValue]:
        """Iterating through all results.

        The difference to the normal iterator is that this iterator will iterate
        through all results, not only the direct children. This is useful when
        iterating through results of a wildcard or partial node.

        Returns:
            Results iterator.
        """
        for path, value in self._value_structure.items():
            if path.startswith(self.path):
                yield value

    def try_generate_subnode_result(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> ResultNode | AnnotatedValue:
        """Provides nodes for the extended path or the original values for leafs.

        Will fail if the resulting Path is ill-formed.

        Args:
            next_path_segment: Segment, with which the current path should be
                extended.

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.

        Raises:
            LabOneInvalidPathError: If the extension leads to an invalid path or if it
                is tried to use wildcards in ResultNodes.
        """
        if next_path_segment == WILDCARD:
            msg = (
                    f"Wildcards '*' are not allowed in a tree representing "
                    f"measurement results. However, it was tried to extend {self.path} "
                    f"with a wildcard."
                )
            raise LabOneInvalidPathError(msg) from e
        
        sub_node = self.try_generate_subnode(next_path_segment)

        if sub_node.children:
            return ResultNode(
                tree_data=self.tree_data,
                capnp_struct=sub_node.capnp_struct,
                parametrization=sub_node.parametrization,
                abstract_path_segments=(*sub_node.path_segments, sub_node.capnp_struct.name),
                value_structure=self._value_structure,
                timestamp=self._timestamp,
            )
        
        # plain result for leafs
        return self._value_structure[sub_node.path]


class Node(MetaNode, ABC):
    """Single node in the object-based node tree.

    The child nodes of each node can be accessed either by attribute or by item.

    The core functionality of each node is the overloaded call operator.
    Making a call gets the value(s) for that node. Passing a value to the call
    operator will set that value to the node on the device. Calling a node that
    is not a leaf (wildcard or partial node) will return/set the value on every
    node that matches it.

    Warning:
        Setting a value to a non-leaf node will try to set the value of all
        nodes that matches that node. It should therefore be used with great care
        to avoid unintentional changes.

    In addition to the call operator every node has a `wait_for_state_change`
    function that can be used to wait for a node to change state.

    Leaf nodes also have a `subscribe` function that can be used to subscribe to
    changes in the node. For more information on the subscription functionality
    see the documentation of the `subscribe` function or the `DataQueue` class.
    """

    def build(
            tree_data: TreeData,
        capnp_struct,
        parametrization: list[int],
        abstract_path_segments: tuple[NormalizedPathSegment, ...],
        is_wildcard: bool,
        ) -> WildcardOrPartialNode:
        if is_wildcard:
            return WildcardNode(
                tree_data=tree_data,
                capnp_struct=capnp_struct,
                parametrization=parametrization,
                abstract_path_segments=abstract_path_segments,
            )

        if capnp_struct.subNodes:
            return PartialNode(
                tree_data=tree_data,
                capnp_struct=capnp_struct,
                parametrization=parametrization,
                abstract_path_segments=abstract_path_segments,
            )
        return LeafNode(
            tree_data=tree_data,
            capnp_struct=capnp_struct,
            parametrization=parametrization,
            abstract_path_segments=abstract_path_segments,
        )

    def __getattr__(self, next_segment: str) -> Node:
        """Access sub-node.

        ```python
        node.demods  # where 'demods' is the next segment to enter
        ```

        Args:
            next_segment: Segment, with which the current path should be extended

        Returns:
            New node-object, representing the extended path, or plain value,
            if came to a leaf-node.
        """
        return self.try_generate_subnode(normalize_path_segment(next_segment))

    def __getitem__(self, path_extension: str | int) -> Node:
        """Go one or multiple levels deeper into the tree structure.

        The primary purpose of this operator is to allow accessing subnodes that
        are numbers (e.g. /0, /1, ...). The attribute access operator (dot) does
        not allow this, because numbers are not valid identifiers in Python.

        However, this operator can deal with a number of different scenarios:

        - simple path extensions:

            ```python
            node['deeper']
            ```

        - path indexing:

            ```python
            node['deeper/path']
            ```

        - numeric indexing:

            ```python
            node[0]
            ```

        - wildcards (placeholder for multiple path-extensions):

            ```python
            node['*']
            ```

        - combinations of all that:

            ```python
            node['deeper/*/path/0']
            ```


        All these implementations are equivalent:

        ```python
        node['mds/groups/0']
        node['mds']['groups'][0]
        node.mds.groups[0]
        ```

        Args:
            path_extension: path, number or wildcard.

        Returns: New node-object, representing the extended path
        """
        relative_path_segments = split_path(str(path_extension))
        current_node = self

        for path_segment in relative_path_segments:
            current_node = current_node.try_generate_subnode(
                normalize_path_segment(path_segment),
            )

        return current_node

    def __eq__(self, other: object) -> bool:
        return (
            other.__class__ == self.__class__
            and self.path_segments == other.path_segments  # type:ignore[attr-defined]
            and self.tree_data == other.tree_data  # type:ignore[attr-defined]
        )

    def __hash__(self) -> int:
        return hash((self.path, hash(self.__class__), hash(self._tree_data)))

    def __contains__(self, item: str | int | Node) -> bool:
        """Checks if a path-segment or node is an immediate sub-node of this one.

        Args:
            item: To be checked this is among the child-nodes. Can be called with
                either a node, or a plain identifier/number, which would be used
                to identify the child.

        Returns:
            If item describes/is a valid subnode.

        Example:
            >>> if "debug" in node:         # implicit call to __contains__
            >>>     print(node["debug"])    # if contained, indexing is valid
            >>>     print(node.debug)       # ... as well as dot-access

            Nodes can also be used as arguments. In this example, it is asserted
            that all subnodes are "contained" in the node.
            >>> for subnode in node:        # implicit call to __iter__
            >>>     assert subnode in node  # implicit call to __contains__
        """
        if isinstance(item, Node):
            return self.is_child_node(item)
        return normalize_path_segment(item) in self.segment_to_subnode

    async def __call__(
        self,
        value: Value | None = None,
    ) -> AnnotatedValue | ResultNode:
        """Call with or without a value for setting/getting the node.

        Args:
            value: optional value, which is set to the node. If it is omitted,
                a get request is triggered instead.

        Returns:
            The current value of the node is returned either way. If a set-request
                is triggered, the new value will be given back. In case of non-leaf
                nodes, a node-structure representing the results of all sub-paths is
                returned.

        Raises:
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not writeable or readable.
            UnimplementedError: If the get or set request is not supported
                by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        if value is None:
            return await self._get()

        return await self._set(value)

    @abstractmethod
    async def _get(
        self,
    ) -> AnnotatedValue | ResultNode:
        ...

    @abstractmethod
    async def _set(
        self,
        value: Value,
    ) -> AnnotatedValue | ResultNode:
        ...

    @abstractmethod
    async def wait_for_state_change(
        self,
        value: int | NodeEnum,
        *,
        invert: bool = False,
    ) -> None:
        """Waits until the node has the expected state/value.

        Warning:
            Only supports integer and keyword nodes. (The value can either be the value
            or its corresponding enum value as string)

        Args:
            value: Expected value of the node.
            invert: Instead of waiting for the value, the function will wait for
                any value except the passed value. (default = False)
                Useful when waiting for value to change from existing one.
        """
        ...

    @t.overload
    async def subscribe(self, *, get_initial_value: bool = False) -> DataQueue:
        ...

    @t.overload
    async def subscribe(
        self,
        *,
        queue_type: type[QueueProtocol],
        get_initial_value: bool = False,
    ) -> QueueProtocol:
        ...

    @abstractmethod
    async def subscribe(
        self,
        *,
        queue_type: type[QueueProtocol] | None = None,
        get_initial_value: bool = False,
    ) -> QueueProtocol | DataQueue:
        """Subscribe to a node.

        Subscribing to a node will cause the server to send updates that happen
        to the node to the client. The updates are sent to the client automatically
        without any further interaction needed. Every update will be put into the
        queue, which can be used to receive the updates.

        Warning:
            Currently one can only subscribe to nodes that are leaf-nodes.

        Note:
            A node can be subscribed multiple times. Each subscription will
            create a new queue. The queues are independent of each other. It is
            however recommended to only subscribe to a node once and then fork
            the queue into multiple independent queues if needed. This will
            prevent unnecessary network traffic.

        Note:
            There is no explicit unsubscribe function. The subscription will
            automatically be cancelled when the queue is closed. This will
            happen when the queue is garbage collected or when the queue is
            closed manually.

        Args:
            queue_type: The type of the queue to be returned. This can be
                any class matching the DataQueue interface. Only needed if the
                default DataQueue class is not sufficient. If None is passed
                the default DataQueue class is used. (default=None)
            get_initial_value: If True, the initial value of the node is
                is placed in the queue. (default=False)

        Returns:
            A DataQueue, which can be used to receive any changes to the node in a
            flexible manner.
        """



class LeafNode(Node):
    """Node corresponding to a leaf in the path-structure."""

    async def _get(self) -> AnnotatedValue:
        """Get the value of the node.

        Returns:
            The current value of the node.

        Raises:
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not readable.
            UnimplementedError: If the get request is not supported
                by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        return self._tree_data.parser(
            await self._tree_data.session.get(self.path),
        )

    async def _set(
        self,
        value: Value,
    ) -> AnnotatedValue:
        """Set the value of the node.

        Args:
            value: Value, which should be set to the node.

        Returns:
            The new value of the node.

        Raises:
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not settable.
            UnimplementedError: If the set request is not
                supported by the server.
            InternalError: If an unexpected internal error occurs.
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        return self._tree_data.parser(
            await self._tree_data.session.set(
                AnnotatedValue(value=value, path=self.path),
            ),
        )

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> Node:
        """Provides nodes for the extended path or the original values for leafs.

        Args:
            next_path_segment: Segment, with which the current path should be
                extended.

        Returns:
            New node-object, representing the extended path.

        Raises:
            LabOneInvalidPathError: Always, because extending leaf-paths is illegal.

        """
        msg = (
            f"Node '{self.path}' cannot be extended with "
            f"'/{next_path_segment}' because it is a leaf node."
        )
        raise LabOneInvalidPathError(msg)

    @t.overload
    async def subscribe(self, *, get_initial_value: bool = False) -> DataQueue:
        ...

    @t.overload
    async def subscribe(
        self,
        *,
        queue_type: type[QueueProtocol],
        get_initial_value: bool = False,
    ) -> QueueProtocol:
        ...

    async def subscribe(
        self,
        *,
        queue_type: type[QueueProtocol] | None = None,
        get_initial_value: bool = False,
    ) -> QueueProtocol | DataQueue:
        """Subscribe to a node.

        Subscribing to a node will cause the server to send updates that happen
        to the node to the client. The updates are sent to the client automatically
        without any further interaction needed. Every update will be put into the
        queue, which can be used to receive the updates.

        Note:
            A node can be subscribed multiple times. Each subscription will
            create a new queue. The queues are independent of each other. It is
            however recommended to only subscribe to a node once and then fork
            the queue into multiple independent queues if needed. This will
            prevent unnecessary network traffic.

        Note:
            There is no explicit unsubscribe function. The subscription will
            automatically be cancelled when the queue is closed. This will
            happen when the queue is garbage collected or when the queue is
            closed manually.

        Args:
            queue_type: The type of the queue to be returned. This can be
                any class matching the DataQueue interface. Only needed if the
                default DataQueue class is not sufficient. If None is passed
                the default DataQueue class is used. (default=None)

            get_initial_value: If True, the initial value of the node is
                is placed in the queue. (default=False)

        Returns:
            A DataQueue, which can be used to receive any changes to the node in a
            flexible manner.
        """
        return await self._tree_data.session.subscribe(
            self.path,
            parser_callback=self._tree_data.parser,
            queue_type=queue_type or DataQueue,
            get_initial_value=get_initial_value,
        )

    async def wait_for_state_change(
        self,
        value: int | NodeEnum,
        *,
        invert: bool = False,
    ) -> None:
        """Waits until the node has the expected state/value.

        Warning:
            Only supports integer and keyword nodes. (The value can either be the value
            or its corresponding enum value as string)

        Args:
            value: Expected value of the node.
            invert: Instead of waiting for the value, the function will wait for
                any value except the passed value. (default = False)
                Useful when waiting for value to change from existing one.
        """
        await self._tree_data.session.wait_for_state_change(
            self.path,
            value,
            invert=invert,
        )

    @cached_property
    def node_info(self) -> NodeInfo:
        """Additional information about the node."""
        return self.capnp_struct.info # todo


class WildcardOrPartialNode(Node, ABC):
    """Common functionality for wildcard and partial nodes."""

    async def _get(
        self,
    ) -> ResultNode:
        """Get the value of the node.

        Raises:
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not readable.
            UnimplementedError: If the get with expression request is not
                supported by the server.
            InternalError: If an unexpected internal error occurs
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        return self._package_response(
            await self._tree_data.session.get_with_expression(self.path),
        )

    async def _set(
        self,
        value: Value,
    ) -> ResultNode:
        """Set the value of the node.

        Args:
            value: Value, which should be set to the node.

        Raises:
            OverwhelmedError: If the kernel is overwhelmed.
            BadRequestError: If the path is not readable.
            UnimplementedError: If the get with expression request is not
                supported by the server.
            InternalError: If an unexpected internal error occurs
            LabOneCoreError: If something else went wrong that can not be
                mapped to one of the other errors.
        """
        return self._package_response(
            await self._tree_data.session.set_with_expression(
                AnnotatedValue(value=value, path=self.path),
            ),
        )

    def _package_response(
        self,
        raw_response: list[AnnotatedValue],
    ) -> ResultNode:
        """Package server-response of wildcard or partial get-request.

        The result node will start to index from the root of the tree:

        >>> result_node = device.demods["*"].sample["*"].x()
        >>> result_node.demods[0].sample[0].x

        Of course, only the paths matching the wildcard/partial path
        will be available in the result node.

        Args:
            raw_response: server-response to get (or set) request

        Returns:
            Node-structure, representing the results.
        """
        timestamp = raw_response[0].timestamp if raw_response else None

        # replace values by enum values and parse if applicable
        raw_response = [self._tree_data.parser(r) for r in raw_response]

        # package into dict
        response_dict = {
            annotated_value.path: annotated_value for annotated_value in raw_response
        }

        # same starting point as root
        model_node = self.tree_data.root

        return ResultNode(
            tree_data=model_node.tree_data,
            capnp_struct=model_node.capnp_struct,
            parametrization=model_node.parametrization,
            abstract_path_segments=model_node.abstract_path_segments, 
            value_structure=response_dict,
            timestamp=timestamp,
        )

    @t.overload
    async def subscribe(
        self,
        *,
        get_initial_value: bool = False,
    ) -> DataQueue:
        ...

    @t.overload
    async def subscribe(
        self,
        *,
        queue_type: type[QueueProtocol],
        get_initial_value: bool = False,
    ) -> QueueProtocol:
        ...

    async def subscribe(
        self,
        *,
        queue_type: type[QueueProtocol] | None = None,  # noqa: ARG002
        get_initial_value: bool = False,  # noqa: ARG002
    ) -> QueueProtocol | DataQueue:
        """Subscribe to a node.

        Currently not supported for wildcard and partial nodes.

        Raises:
            NotImplementedError: Always.
        """
        msg = (
            "Subscribing to paths with wildcards "
            "or non-leaf paths is not supported. "
            "Subscribe to a leaf node instead "
            "and make sure to not use wildcards in the path."
        )
        raise LabOneNotImplementedError(msg)


class WildcardNode(WildcardOrPartialNode):
    """Node corresponding to a path containing one or more wildcards."""

    def __contains__(self, item: str | int | Node) -> bool:
        msg = (
            "Checking if a wildcard-node contains a subnode is not supported."
            "For checking if a path is contained in a node, make sure to not use"
            "wildcards in the path."
        )
        raise LabOneInappropriateNodeTypeError(msg)

    def __iter__(self) -> t.Iterator[Node]:
        msg = (
            "Iterating through a wildcard-node is not supported. "
            "For iterating through child nodes, make sure to not "
            "use wildcards in the path."
        )
        raise LabOneInappropriateNodeTypeError(msg)

    def __len__(self) -> int:
        msg = (
            "Getting the length of a wildcard-node is not supported."
            "For getting the length of a node, make sure to not "
            "use wildcards in the path."
        )
        raise LabOneInappropriateNodeTypeError(msg)

    def try_generate_subnode(
        self,
        next_path_segment: NormalizedPathSegment,
    ) -> Node:
        """Provides nodes for the extended path or the original values for leafs.

        Will never fail, because wildcard-paths are not checked to have valid matchings.

        Args:
            next_path_segment: Segment, with which the current path should be
                extended.

        Returns:
            New node-object, representing the extended path.

        """
        return WildcardNode(
            tree_data=self.tree_data,
            capnp_struct=self.capnp_struct, # todo: wildcard has kein capnp struct, oder? Schauen dass alles klappt was das bruacht
            parametrization=self.parametrization,
            abstract_path_segments=(*self.abstract_path_segments, next_path_segment),
        )

    async def wait_for_state_change(
        self,
        value: int | NodeEnum,
        *,
        invert: bool = False,
    ) -> None:
        """Waits until all wildcard-associated nodes have the expected state/value.

        Warning:
            Only supports integer and keyword nodes. (The value can either be the value
            or its corresponding enum value as string)

        Args:
            value: Expected value of the node.
            invert: Instead of waiting for the value, the function will wait for
                any value except the passed value. (default = False)
                Useful when waiting for value to change from existing one.
        """
        # find paths corresponding to this wildcard-path and put them into nodes
        resolved_nodes = [
            self.tree_data.get_root(hide_kernel_prefix=False)[path]
            for path in await self.tree_data.session.list_nodes(self.path)
        ]
        await asyncio.gather(
            *[
                node.wait_for_state_change(value, invert=invert)
                for node in resolved_nodes
            ],
        )


class PartialNode(WildcardOrPartialNode):
    """Node corresponding to a path, which has not reached a leaf yet."""

    async def wait_for_state_change(
        self,
        value: int | NodeEnum,  # noqa: ARG002
        *,
        invert: bool = False,  # noqa: ARG002
    ) -> None:
        """Not applicable for partial-nodes.

        Args:
            value: Expected value of the node.
            invert: Instead of waiting for the value, the function will wait for
                any value except the passed value. (default = False)
                Useful when waiting for value to change from existing one.

        Raises:
            LabOneInappropriateNodeTypeError: Always, because partial nodes cannot be
            waited for.
        """
        msg = (
            "Cannot wait for a partial node to change its value. Consider waiting "
            "for a change of one or more leaf-nodes instead."
        )
        raise LabOneInappropriateNodeTypeError(msg)
