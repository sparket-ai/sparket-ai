from __future__ import annotations

import asyncio
import random
import time
from typing import List

import bittensor as bt


class MockSubtensor(bt.MockSubtensor):
    def __init__(self, netuid, n=16, wallet=None, network="mock"):
        super().__init__(network=network)

        # bt 10.x subnet_exists() returns MagicMock; check chain_state directly
        networks = self.chain_state.get("SubtensorModule", {}).get("NetworksAdded", {})
        if netuid not in networks:
            self.create_subnet(netuid)

        # Register ourself (the validator) as a neuron at uid=0
        if wallet is not None:
            hotkey_addr = wallet.hotkey.ss58_address
            # In mock mode, coldkey may be encrypted; use hotkey as fallback
            try:
                coldkey_addr = wallet.coldkey.ss58_address
            except Exception:
                coldkey_addr = hotkey_addr
            self.force_register_neuron(
                netuid=netuid,
                hotkey_ss58=hotkey_addr,
                coldkey_ss58=coldkey_addr,
                balance=100000,
                stake=100000,
            )

        # Register n mock neurons who will be miners
        for i in range(1, n + 1):
            self.force_register_neuron(
                netuid=netuid,
                hotkey_ss58=f"miner-hotkey-{i}",
                coldkey_ss58="mock-coldkey",
                balance=100000,
                stake=100000,
            )


class MockMetagraph(bt.Metagraph):
    def __init__(self, netuid=1, network="mock", subtensor=None):
        super().__init__(netuid=netuid, network=network, sync=False)

        if subtensor is not None:
            self.subtensor = subtensor
        self.sync(subtensor=subtensor)

        for axon in self.axons:
            axon.ip = "127.0.0.0"
            axon.port = 8093

        bt.logging.info(f"Metagraph: {self}")
        bt.logging.info(f"Axons: {self.axons}")


class MockDendrite(bt.Dendrite):
    """Mock dendrite for local testing without network communication."""

    def __init__(self, wallet):
        super().__init__(wallet)

    async def forward(
        self,
        axons: List[bt.Axon],
        synapse: bt.Synapse = bt.Synapse(),
        timeout: float = 12,
        deserialize: bool = True,
        run_async: bool = True,
        streaming: bool = False,
    ):
        if streaming:
            raise NotImplementedError("Streaming not implemented.")

        async def query_all_axons():
            async def single_axon_response(i, axon):
                start_time = time.time()
                s = synapse.copy()
                s = self.preprocess_synapse_for_request(axon, s, timeout)
                
                process_time = random.random()
                if process_time < timeout:
                    s.dendrite.process_time = str(time.time() - start_time)
                    s.dendrite.status_code = 200
                    s.dendrite.status_message = "OK"
                else:
                    s.dendrite.process_time = str(timeout)
                    s.dendrite.status_code = 408
                    s.dendrite.status_message = "Timeout"
                return s

            tasks = []
            for i, axon in enumerate(axons):
                tasks.append(asyncio.create_task(single_axon_response(i, axon)))
            return await asyncio.gather(*tasks)

        return await query_all_axons()
