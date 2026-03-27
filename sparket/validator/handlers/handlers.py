# main handlers module for the validator - instantiates and makes all handlers available to the validator runtime

from typing import Any
from sparket.validator.handlers.ingest.ingest_odds import IngestOddsHandler
from sparket.validator.handlers.ingest.ingest_outcome import IngestOutcomeHandler
from sparket.validator.handlers.score.odds_score import OddsScoreHandler
from sparket.validator.handlers.score.outcome_score import OutcomeScoreHandler
from sparket.validator.handlers.score.main_score import MainScoreHandler
from sparket.validator.handlers.core.weights.set_weights import SetWeightsHandler
from sparket.validator.handlers.core.chain.miner_management import MinerManagementHandler
from sparket.validator.handlers.core.chain.sync_metagraph import SyncMetagraphHandler
from sparket.validator.handlers.data.game_data import GameDataHandler


class Handlers:
    def __init__(self, database: Any, syncer: Any = None):
        self.database = database
        self.ingest_odds_handler = IngestOddsHandler(database)
        self.ingest_outcome_handler = IngestOutcomeHandler(database)
        self.odds_score_handler = OddsScoreHandler(database)
        self.outcome_score_handler = OutcomeScoreHandler(database)
        self.main_score_handler = MainScoreHandler(database, syncer=syncer)
        self.set_weights_handler = SetWeightsHandler(database)
        self.miner_management_handler = MinerManagementHandler(database)
        self.sync_metagraph_handler = SyncMetagraphHandler(database)
        self.game_data_handler = GameDataHandler(database)
