from kioss import _pipe
from kioss._pipe import Pipe
from kioss._visit import _explanation, _iter_production

Pipe.ITERATOR_PRODUCING_VISITOR_CLASS = _iter_production.IteratorProducingVisitor
Pipe.EXPLAINING_VISITOR_CLASS = _explanation.ExplainingVisitor
Pipe.SOURCE_PIPE_CLASS = _pipe.SourcePipe
