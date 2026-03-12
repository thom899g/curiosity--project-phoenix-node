"""
Project Phoenix Node - Core Orchestrator Service
Main coordinator that schedules and monitors all components of the autonomous trading system.
Architectural Choice: Event-driven microservice with Firebase state persistence for 24/7 operation.
"""
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from flask import Flask, jsonify
import schedule
import firebase_admin
from firebase_admin import firestore, db, credentials
from dataclasses import dataclass, asdict
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

@dataclass
class SystemState:
    """Immutable system state container for thread-safe operations"""
    last_data_fetch: Optional[datetime] = None
    last_signal_time: Optional[datetime] = None
    active_position: Optional[Dict[str, Any]] = None
    cycle_count: int = 0
    total_profit_usd: float = 0.0
    consecutive_errors: int = 0
    health_status: str = "HEALTHY"
    current_strategy_id: str = "mean_reversion_v2"

class PhoenixOrchestrator:
    """Main orchestrator with fault tolerance and state persistence"""
    
    def __init__(self, firebase_cred_path: str = 'phoenix-service-account.json'):
        """
        Initialize orchestrator with Firebase backend.
        
        Args:
            firebase_cred_path: Path to Firebase service account credentials
        """
        self._initialize_firebase(firebase_cred_path)
        self.current_state = self._load_persistent_state()
        self.health_metrics = self._initialize_health_metrics()
        self.app = Flask(__name__)
        self._setup_flask_routes()
        self.is_running = False
        self.error_threshold = 5
        
        logger.info("Phoenix Orchestrator initialized with state: %s", self.current_state)
    
    def _initialize_firebase(self, cred_path: str) -> None:
        """Initialize Firebase connection with error handling"""
        try:
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred, {
                'databaseURL': 'https://project-phoenix.firebaseio.com',
                'projectId': 'project-phoenix'
            })
            self.db = firestore.client()
            self.rtdb = db.reference()
            logger.info("Firebase connection established successfully")
        except FileNotFoundError as e:
            logger.error(f"Firebase credentials not found at {cred_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize Firebase: {e}")
            raise
    
    def _load_persistent_state(self) -> SystemState:
        """Load system state from Firestore with default fallback"""
        try:
            state_ref = self.db.collection('system_state').document('orchestrator')
            state_data = state_ref.get()
            
            if state_data.exists:
                data = state_data.to_dict()
                # Convert string timestamps back to datetime objects
                if data.get('last_data_fetch'):
                    data['last_data_fetch'] = datetime.fromisoformat(data['last_data_fetch'])
                if data.get('last_signal_time'):
                    data['last_signal_time'] = datetime.fromisoformat(data['last_signal_time'])
                return SystemState(**data)
        except Exception as e:
            logger.warning(f"Failed to load state from Firestore: {e}")
        
        # Return default state
        return SystemState()
    
    def _save_persistent_state(self) -> None:
        """Save current state to Firestore with timestamp"""
        try:
            state_ref = self.db.collection('system_state').document('orchestrator')
            state_dict = asdict(self.current_state)
            
            # Convert datetime objects to ISO format strings
            for key in ['last_data_fetch', 'last_signal_time']:
                if state_dict[key]:
                    state_dict[key] = state_dict[key].isoformat()
            
            state_dict['last_updated'] = datetime.utcnow().isoformat()
            state_ref.set(state_dict, merge=True)
            logger.debug("System state saved to Firestore")
        except Exception as e:
            logger.error(f"Failed to save state to Firestore: {e}")
            self.current_state.consecutive_errors += 1
    
    def _initialize_health_metrics(self) -> Dict[str, Any]:
        """Initialize health monitoring metrics"""
        return {
            'start_time': datetime.utcnow(),
            'total_cycles': 0,
            'successful_cycles': 0,
            'failed_cycles': 0,
            'component_status': {
                'market_ingestor': 'UNKNOWN',
                'feature_engine': 'UNKNOWN',
                'strategy_engine': 'UNKNOWN',
                'execution_gateway': 'UNKNOWN'
            },
            'last_error': None,
            'uptime_seconds': 0
        }
    
    def _setup_flask_routes(self) -> None:
        """Setup HTTP endpoints for health checks and monitoring"""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """Health check endpoint for monitoring"""
            current_time = datetime.utcnow()
            self.health_metrics['uptime_seconds'] = (
                current_time - self.health_metrics['start_time']
            ).total_seconds()
            
            # Check if system is healthy
            is_healthy = (
                self.current_state.consecutive_errors < self.error_threshold and
                self.current_state.health_status == "HEALTHY"
            )
            
            status_code = 200 if is_healthy else 503
            
            response = {
                'status': 'healthy' if is_healthy else 'unhealthy',
                'timestamp': current_time.isoformat(),
                'uptime_seconds': self.health_metrics['uptime_seconds'],
                'system_state': asdict(self.current_state),
                'health_metrics': self.health_metrics,
                'consecutive_errors': self.current_state.consecutive_errors
            }
            
            return jsonify(response), status_code
        
        @self.app.route('/metrics', methods=['GET'])
        def metrics():
            """Prometheus-style metrics endpoint"""
            from prometheus_client import generate_latest, CollectorRegistry, Gauge, Counter
            
            registry = CollectorRegistry()
            
            # Define metrics
            uptime = Gauge('phoenix_uptime_seconds', 'System uptime in seconds', registry=registry)
            cycles = Counter('phoenix_cycles_total', 'Total cycles completed', registry=registry)
            errors = Counter('phoenix_errors_total', 'Total errors encountered', registry=registry)
            profit = Gauge('phoenix_profit_usd', 'Total profit in USD', registry=registry)
            
            # Set metric values
            uptime.set(self.health_metrics['uptime_seconds'])
            cycles.inc(self.health_metrics['total_cycles'])
            errors.inc(self.health_metrics['failed_cycles'])
            profit.set(self.current_state.total_profit_usd)
            
            return generate_latest(registry), 200, {'Content-Type': 'text/plain'}
    
    def schedule_jobs(self) -> None:
        """Schedule periodic jobs for the trading loop"""
        # Market data fetch every hour at minute 5
        schedule.every().hour.at(":05").do(self._execute_market_ingestion)
        
        # Strategy evaluation every hour at minute 10
        schedule.every().hour.at(":10").do(self._execute_strategy_cycle)
        
        # Health check and state save every 30 minutes
        schedule.every(30).minutes.do(self._health_check_cycle)
        
        # Daily performance report at midnight UTC
        schedule.every().day.at("00:00").do(self._generate_daily_report)
        
        logger.info("Scheduled jobs initialized")
    
    def _execute_market_ingestion(self) -> None:
        """Execute market data ingestion cycle"""
        try:
            logger.info("Starting market ingestion cycle")
            
            # Import here to avoid circular imports
            from market_ingestor import MarketIngestor
            
            ingestor = MarketIngestor(exchange_id='binance')
            data = ingestor.fetch_ohlcv(symbol='BTC/USDT', timeframe='1h', limit=100)
            
            if data is not None:
                self.current_state.last_data_fetch = datetime.utcnow()
                self.health_metrics['component_status']['market_ingestor'] = 'HEALTHY'
                self.health_metrics['successful_cycles'] += 1
                logger.info(f"Market data fetched successfully: {len(data)} records")
            else:
                raise ValueError("Market ingestor returned None")
                
        except Exception as e:
            logger.error(f"Market ingestion failed: {e}")
            self.health_metrics['component_status']['market_ingestor'] = 'ERROR'
            self.health_metrics['failed_cycles'] += 1
            self.current_state.consecutive_errors += 1
            self._handle_error(e)
    
    def _execute_strategy_cycle(self) -> None:
        """Execute complete strategy cycle: features -> signal -> execution"""
        try:
            logger.info("Starting strategy cycle")
            
            # 1. Calculate features
            from feature_engine import FeatureEngine
            feature_engine = FeatureEngine()
            
            # Get latest data from Firestore
            market_data_ref = self.db.collection('market_data')
            query = market_data_ref.order_by('timestamp', direction=firestore.Query.DESCENDING).limit(100)
            docs = query.stream()
            
            data = []
            for doc in docs:
                data.append(doc.to_dict())
            
            if not data:
                raise ValueError("No market data available for feature calculation")
            
            # Calculate features
            features_df = feature_engine.calculate_features(data, 'BTC/USDT')
            self.health_metrics['component_status']['feature_engine'] = 'HEALTHY'
            
            # 2. Generate signal
            from strategy_engine import MeanReversionStrategy
            strategy = MeanReversionStrategy()
            signal = strategy.generate_signal(features_df)
            
            # Store signal decision
            decision_ref = self.db.collection('decisions').document()
            decision_ref.set({
                'signal': signal,
                'timestamp': datetime.utcnow().isoformat(),
                'strategy_id': strategy.strategy_id,
                'strategy_version': strategy.version
            })
            
            self.current_state.last_signal_time = datetime.utcnow()
            self.health_metrics['component_status']['strategy_engine'] = 'HEALTHY'
            
            # 3. Execute if signal is strong enough
            if signal['action'] in ['BUY', 'SELL'] and signal['confidence'] > 0.7:
                logger.info(f"Strong signal detected: {signal['action']} with confidence {signal['confidence']:.2f}")
                self._execute_trade(signal)
            
            self.current_state.cycle_count += 1
            self.health_metrics['total_cycles'] += 1
            self.health_metrics['successful_cycles'] += 1
            self.current_state.consecutive_errors = 0
            
        except Exception as e:
            logger.error(f"Strategy cycle failed: {e}")
            self.health_metrics['failed_cycles'] += 1
            self.current_state.consecutive_errors += 1
            self._handle_error(e)
    
    def _execute_trade(self, signal: Dict[str, Any]) -> None:
        """Execute trade through execution gateway"""
        try:
            # In production, this would use Alpaca API
            # For now, simulate trade execution
            from execution_gateway import ExecutionGateway
            gateway = ExecutionGateway()
            
            # Execute trade
            execution_result = gateway.execute_order(
                symbol='BTC/USDT',
                side=signal['action'].lower(),
                quantity=0.001,  # 0.001 BTC for safety
                price=signal['price']
            )
            
            # Update position state
            self.current_state.active_position = {
                'symbol': 'BTC/USDT',
                'side': signal['action'].lower(),
                'entry_price': signal['price'],
                'quantity': 0.001,
                'entry_time': datetime.utcnow().isoformat(),
                'signal_id': signal.get('id')
            }
            
            # Store execution record
            execution_ref = self.db.collection('executions').document()
            execution_ref.set({
                **execution_result,
                'timestamp': datetime.utcnow().isoformat(),
                'signal': signal
            })
            
            self.health_metrics['component_status']['execution_gateway'] = 'HEALTHY'
            logger.info(f"Trade executed: {signal['action']} at {signal['price']}")
            
        except Exception as e:
            logger.error(f"Trade execution failed: {e}")
            self.health_metrics['component_status']['execution_gateway'] = 'ERROR'
            raise
    
    def _health_check_cycle(self) -> None:
        """Perform system health check and save state"""
        try:
            # Update health metrics
            current_time = datetime.utcnow()
            self.health_metrics['uptime_seconds'] = (
                current_time - self.health_metrics['start_time']
            ).total