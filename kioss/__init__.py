from kioss._plan import APipe, SourcePipe as Pipe
from kioss._util import LOGGER
from kioss import _plan, _visitor
_plan.APipe.ITERATOR_GENERATING_VISITOR_CLASS = _visitor.IteratorGeneratingVisitor