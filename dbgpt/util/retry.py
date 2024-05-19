import asyncio
import logging
import traceback

from dbgpt.app.scene.exceptions import AppActionException
from dbgpt.core import ModelRequest

logger = logging.getLogger(__name__)


def async_retry(
    retries: int = 1, parallel_executions: int = 1, catch_exceptions=(Exception,)
):
    """Async retry decorator.

    Examples:
        .. code-block:: python

            @async_retry(retries=3, parallel_executions=2)
            async def my_func():
                # Some code that may raise exceptions
                pass

    Args:
        retries (int): Number of retries.
        parallel_executions (int): Number of parallel executions.
        catch_exceptions (tuple): Tuple of exceptions to catch.
    """

    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_exception = None
            hints = []
            content = None
            for attempt in range(retries):
                # attach hints from db to prompt
                if (
                    len(hints) > 0
                    and len(args) > 0
                    and isinstance(args[1], ModelRequest)
                ):
                    payload = args[1]
                    if len(payload.messages) >= 1:
                        question = payload.messages[-1]
                        if not content:
                            system = payload.messages[0]
                            # deduplicate message when retrying
                            if system.content in question.content:
                                content = question.content[len(system.content)]
                            else:
                                content = question.content
                        notice = "Note, please avoid following erros in your SQL:"
                        msg = "\n".join(hints)
                        question.content = f"{content}\n{notice}\n{msg}"

                tasks = [func(*args, **kwargs) for _ in range(parallel_executions)]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if not isinstance(result, Exception):
                        return result
                    if isinstance(result, catch_exceptions):
                        last_exception = result
                        logger.error(
                            f"Attempt {attempt + 1} of {retries} failed with error: "
                            f"{type(result).__name__}, {str(result)}"
                        )
                        logger.debug(traceback.format_exc())

                    # error message from db as a hint
                    if isinstance(result, AppActionException) and len(args) > 1:
                        msg = result.view
                        if "https://sqlalche.me/" in msg:
                            msg = msg.split("</span>")[-1]
                            msg = msg.split("https://sqlalche.me/")[0]
                            msg = msg.split("(Background on this error at:")[0]
                            msg = msg.split(", SQL:")[0]
                            if "ERROR, Message:" in msg:
                                msg = msg.split("ERROR, Message:")[1]
                            msg = msg.split("Routine: ")[0]
                            hints.append(msg)

                logger.info(f"Retrying... (Attempt {attempt + 1} of {retries})")

            raise last_exception  # After all retries, raise the last caught exception

        return wrapper

    return decorator
