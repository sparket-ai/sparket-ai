# handler class for scoring all odds and outcomes submissions across all miners -> validator database (this will be the main scoring handler, runs periodically, updates final weights to be submitted to chain)

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import bittensor as bt

from sparket.validator.scoring.jobs.rolling_aggregates import RollingAggregatesJob
from sparket.validator.scoring.jobs.calibration_sharpness import CalibrationSharpnessJob
from sparket.validator.scoring.jobs.originality_lead_lag import OriginalityLeadLagJob
from sparket.validator.scoring.jobs.skill_score import SkillScoreJob
from sparket.validator.scoring.ground_truth.snapshot_pipeline import SnapshotPipeline
from sparket.validator.handlers.score.odds_score import OddsScoreHandler
from sparket.validator.handlers.score.outcome_score import OutcomeScoreHandler


class MainScoreHandler:
    """Main scoring orchestrator.
    
    Runs the complete scoring pipeline in dependency order:
    1. Ground truth snapshot pipeline (consensus from provider quotes)
    2. Rolling aggregates (time-decayed metrics per miner)
    3. Calibration + Sharpness (calibration curve, forecast variance)
    4. Originality + Lead-lag (independence, information advantage)
    5. Skill score (final normalized composite)
    """

    def __init__(self, database: Any):
        self.database = database
        self._logger = logging.getLogger("main_score")

    async def run(self, emit_weights: bool = False, validator: Optional[Any] = None) -> dict:
        """Execute the full scoring pipeline.
        
        Args:
            emit_weights: If True and validator provided, emit weights to chain
            validator: Validator instance (required for weight emission)
            
        Returns:
            Summary dict with job results
        """
        bt.logging.info({"main_score": "start"})
        start_time = time.time()
        
        results = {
            "snapshots": 0,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "errors": [],
        }
        
        try:
            # Phase 1: Ground truth snapshot pipeline
            try:
                pipeline = SnapshotPipeline(db=self.database, logger=bt.logging)
                snapshot_count = await pipeline.run_snapshot_cycle()
                closing_count = await pipeline.capture_closing_snapshots()
                # Capture late closing snapshots for events that were missed
                late_closing_count = await pipeline.capture_late_closing_snapshots()
                gt_count = await pipeline.populate_ground_truth_closing()
                
                results["snapshots"] = snapshot_count
                results["closing_snapshots"] = closing_count
                results["late_closing"] = late_closing_count
                results["ground_truth"] = gt_count
                
                bt.logging.info({
                    "main_score_snapshots": {
                        "snapshots": snapshot_count,
                        "closing": closing_count,
                        "late_closing": late_closing_count,
                        "ground_truth": gt_count,
                    }
                })
            except Exception as e:
                bt.logging.warning({"main_score_snapshot_error": str(e)})
                results["errors"].append(f"snapshot_pipeline: {e}")

            # Phase 1.5: Batch score unscored submissions (CLV and outcomes)
            try:
                since = datetime.now(timezone.utc) - timedelta(days=7)
                
                # Score CLV for submissions with ground truth closing
                odds_handler = OddsScoreHandler(self.database)
                clv_scored = await odds_handler.score_batch(since=since, limit=5000)
                results["clv_scored"] = clv_scored
                
                # Score outcomes for submissions with settled markets
                outcome_handler = OutcomeScoreHandler(self.database)
                outcome_scored = await outcome_handler.score_batch(since=since, limit=5000)
                results["outcome_scored"] = outcome_scored
                
                bt.logging.info({
                    "main_score_batch": {
                        "clv_scored": clv_scored,
                        "outcome_scored": outcome_scored,
                    }
                })
            except Exception as e:
                bt.logging.warning({"main_score_batch_error": str(e)})
                results["errors"].append(f"batch_scoring: {e}")

            # Phase 2: Run scoring jobs in dependency order
            job_classes = [
                RollingAggregatesJob,
                CalibrationSharpnessJob,
                OriginalityLeadLagJob,
                SkillScoreJob,
            ]
            
            for job_class in job_classes:
                job_name = job_class.__name__
                try:
                    job = job_class(self.database, self._logger)
                    await job.run()
                    results["jobs_completed"] += 1
                    bt.logging.info({"main_score_job": job_name, "status": "completed"})
                except Exception as e:
                    results["jobs_failed"] += 1
                    results["errors"].append(f"{job_name}: {e}")
                    bt.logging.warning({
                        "main_score_job": job_name,
                        "status": "failed",
                        "error": str(e)
                    })

            # Phase 3: Emit weights if requested
            if emit_weights and validator is not None:
                try:
                    from sparket.validator.handlers.core.weights.set_weights import SetWeightsHandler
                    weights_handler = SetWeightsHandler(self.database)
                    await weights_handler.set_weights_from_db(validator)
                    results["weights_emitted"] = True
                    bt.logging.info({"main_score_weights": "emitted"})
                except Exception as e:
                    results["weights_emitted"] = False
                    results["errors"].append(f"weight_emission: {e}")
                    bt.logging.warning({"main_score_weights_error": str(e)})

            elapsed = time.time() - start_time
            results["elapsed_sec"] = round(elapsed, 2)
            
            bt.logging.info({
                "main_score": "completed",
                "elapsed_sec": results["elapsed_sec"],
                "jobs_completed": results["jobs_completed"],
                "jobs_failed": results["jobs_failed"],
            })
            
            return results
            
        except Exception as e:
            bt.logging.error({"main_score_error": str(e)})
            results["errors"].append(f"main: {e}")
            return results
        finally:
            bt.logging.info({"main_score": "end"})

    async def run_snapshots(self) -> dict:
        """Run only the ground truth snapshot pipeline."""
        bt.logging.info({"main_score_snapshots": "start"})
        results = {"snapshots": 0, "closing_snapshots": 0, "late_closing": 0, "ground_truth": 0, "errors": []}
        try:
            pipeline = SnapshotPipeline(db=self.database, logger=bt.logging)
            snapshot_count = await pipeline.run_snapshot_cycle()
            closing_count = await pipeline.capture_closing_snapshots()
            # Capture late closing snapshots for events that were missed
            late_closing_count = await pipeline.capture_late_closing_snapshots()
            gt_count = await pipeline.populate_ground_truth_closing()
            results["snapshots"] = snapshot_count
            results["closing_snapshots"] = closing_count
            results["late_closing"] = late_closing_count
            results["ground_truth"] = gt_count
            bt.logging.info({
                "main_score_snapshots": {
                    "snapshots": snapshot_count,
                    "closing": closing_count,
                    "late_closing": late_closing_count,
                    "ground_truth": gt_count,
                }
            })
        except Exception as e:
            bt.logging.warning({"main_score_snapshots_error": str(e)})
            results["errors"].append(str(e))
        return results


async def run_main_scoring_if_due_async(
    *,
    step: int,
    app_config: Any,
    handlers: Any,
    validator: Any = None,
    emit_weights: bool = True,
) -> None:
    """
    Step-based scheduler for main scoring. If due, runs the main scoring handler.
    - step: current validator step counter
    - app_config: validator application config (expects .core.timers.main_score_steps)
    - handlers: Handlers instance with .main_score_handler
    - validator: Validator instance (required for weight emission)
    - emit_weights: If True, emit weights after scoring
    """
    try:
        steps_interval = 25
        try:
            steps_interval = int(getattr(getattr(app_config, "core", None), "timers", None).main_score_steps)  # type: ignore[attr-defined]
        except Exception:
            pass
        if steps_interval > 0 and (step % steps_interval == 0):
            try:
                await handlers.main_score_handler.run(
                    emit_weights=emit_weights,
                    validator=validator,
                )
            except Exception as e:
                bt.logging.warning({"main_score_error": str(e)})
    except Exception:
        # fail-closed without affecting the main loop
        pass


async def run_snapshot_pipeline_if_due_async(*, step: int, app_config: Any, handlers: Any) -> None:
    """
    Step-based scheduler for ground truth snapshots only.
    Uses the same interval as main scoring to keep cadence aligned.
    """
    try:
        steps_interval = 25
        try:
            steps_interval = int(getattr(getattr(app_config, "core", None), "timers", None).main_score_steps)  # type: ignore[attr-defined]
        except Exception:
            pass
        if steps_interval > 0 and (step % steps_interval == 0):
            try:
                await handlers.main_score_handler.run_snapshots()
            except Exception as e:
                bt.logging.warning({"main_score_snapshots_error": str(e)})
    except Exception:
        # fail-closed without affecting the main loop
        pass
