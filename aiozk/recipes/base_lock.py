import asyncio
import contextlib
import logging
import time

from aiozk import exc, states

from .sequential import SequentialRecipe


log = logging.getLogger(__name__)


class BaseLock(SequentialRecipe):

    async def wait_in_line(self, znode_label, timeout=None, blocked_by=None):
        try:
            return await asyncio.wait_for(self._wait_in_line(znode_label, blocked_by), timeout,
                                          loop=self.client.loop)
        except asyncio.TimeoutError:
            raise exc.TimeoutError

    async def _wait_in_line(self, znode_label, blocked_by):
        await self.create_unique_znode(znode_label)

        while True:
            owned_positions, contenders = await self.analyze_siblings()
            if znode_label not in owned_positions:
                raise exc.SessionLost

            blockers = contenders[:owned_positions[znode_label]]
            if blocked_by:
                blockers = [
                    contender for contender in blockers
                    if self.determine_znode_label(contender) in blocked_by
                ]

            if not blockers:
                break

            await self.wait_on_sibling(blockers[-1])

        return self.make_contextmanager(znode_label)

    def make_contextmanager(self, znode_label):
        state = {"acquired": True}

        def still_acquired():
            return state["acquired"]

        async def handle_session_loss():
            await self.client.session.state.wait_for(states.States.LOST, loop=self.client.loop)
            if not state["acquired"]:
                return

            log.warning(
                "Session expired at some point, lock %s no longer acquired.",
                self
            )
            state["acquired"] = False

        asyncio.ensure_future(handle_session_loss(), loop=self.client.loop)

        async def on_exit():
            state["acquired"] = False
            await self.delete_unique_znode(znode_label)

        class Lock:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                await on_exit()

        return Lock()


class LockLostError(exc.ZKError):
    pass
