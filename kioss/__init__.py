from kioss._plan import APipe, SourcePipe as Pipe
from kioss._util import LOGGER
from kioss import _plan, _visitor
_plan.ITERATOR_GENERATING_VISITOR = _visitor.IteratorGeneratingVisitor()