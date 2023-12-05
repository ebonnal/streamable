from kioss._pipe import Pipe
from kioss import _pipe
from kioss._util import LOGGER
from kioss._visit import _iter_production, _explanation

Pipe.ITERATOR_PRODUCING_VISITOR_CLASS = _iter_production.IteratorProducingVisitor
Pipe.EXPLAINING_VISITOR_CLASS = _explanation.ExplainingVisitor
Pipe.SOURCE_PIPE_CLASS = _pipe.SourcePipe
