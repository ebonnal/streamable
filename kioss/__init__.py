from kioss._pipe import APipe, SourcePipe as Pipe
from kioss._util import LOGGER
from kioss import _pipe, _visitor
_pipe.ITERATOR_GENERATING_VISITOR = _visitor.IteratorGeneratingVisitor()
_pipe.EXPLAINING_VISITOR_CLASS = _visitor.ExplainingVisitor