from kioss._pipe import APipe, SourcePipe as Pipe
from kioss._util import LOGGER
from kioss import _pipe
from kioss._visit import _explanation, _iter_production

_pipe.ITERATOR_PRODUCING_VISITOR = _iter_production.IteratorProducingVisitor()
_pipe.EXPLAINING_VISITOR_CLASS = _explanation.ExplainingVisitor
