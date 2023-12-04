from kioss._pipe import APipe, SourcePipe as Pipe
from kioss._util import LOGGER
from kioss import _pipe
from kioss._visitors import _explain, _iterator_generation
_pipe.ITERATOR_GENERATING_VISITOR = _iterator_generation.IteratorGeneratingVisitor()
_pipe.EXPLAINING_VISITOR_CLASS = _explain.ExplainingVisitor