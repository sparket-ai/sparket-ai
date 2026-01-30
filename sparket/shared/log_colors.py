"""Terminal color codes for log differentiation.

Usage:
    from sparket.shared.log_colors import LogColors
    
    bt.logging.warning(f"{LogColors.MINER_LABEL} token_invalid: ...")
    bt.logging.error(f"{LogColors.VALIDATOR_LABEL} internal error: ...")
"""


class LogColors:
    """ANSI terminal colors for differentiating log sources."""
    
    RESET = "\033[0m"
    
    # Miner-side issues (yellow - external, not our fault)
    MINER = "\033[93m"
    MINER_LABEL = f"{MINER}[MINER]{RESET}"
    
    # Validator-side issues (red - internal, needs attention)  
    VALIDATOR = "\033[91m"
    VALIDATOR_LABEL = f"{VALIDATOR}[VALIDATOR]{RESET}"
    
    # Network/external services (cyan)
    NETWORK = "\033[96m"
    NETWORK_LABEL = f"{NETWORK}[NETWORK]{RESET}"
    
    # Success/info (green)
    SUCCESS = "\033[92m"
    SUCCESS_LABEL = f"{SUCCESS}[OK]{RESET}"
    
    # Chain/blockchain operations (magenta)
    CHAIN = "\033[95m"
    CHAIN_LABEL = f"{CHAIN}[CHAIN]{RESET}"
