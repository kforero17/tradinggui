from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTableView, QPushButton, QLineEdit, QLabel, QComboBox,
    QHeaderView, QMessageBox
)
from PyQt6.QtCore import Qt, QSortFilterProxyModel
from PyQt6.QtGui import QStandardItemModel, QStandardItem
import pandas as pd
import sys
from typing import Optional
from loguru import logger
from src.data.database import db
from src.config.settings import settings

class StockMetricsModel(QStandardItemModel):
    """Custom model for displaying stock metrics data."""
    
    def __init__(self):
        super().__init__()
        self.setHorizontalHeaderLabels([
            "Ticker", "Last Price", "MA100", "EMA100", "% Above MA100",
            "P/E Ratio", "P/B Ratio", "P/S Ratio", "PEG Ratio", "Forward P/E",
            "Market Cap", "Enterprise Value", "EBITDA", "EBITDA/EV",
            "Updated At"
        ])
        self.load_data()
    
    def load_data(self):
        """Load data from database into the model."""
        try:
            df = db.get_latest_metrics()
            if df.empty:
                logger.warning("No data found in database")
                return
            
            # Format numeric columns
            numeric_cols = ['last_price', 'ma_100', 'ema_100', 'pct_above_ma_100',
                          'pe_ratio', 'pb_ratio', 'ps_ratio', 'peg_ratio', 'forward_pe',
                          'market_cap', 'enterprise_value', 'ebitda', 'ebitda_ev']
            
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    if col in ['market_cap', 'enterprise_value', 'ebitda']:
                        # Format large numbers in billions
                        df[col] = df[col].apply(lambda x: f"${x/1e9:.2f}B" if pd.notnull(x) else "N/A")
                    else:
                        df[col] = df[col].apply(lambda x: f"{x:.2f}" if pd.notnull(x) else "N/A")
            
            # Format date
            df['updated_at'] = pd.to_datetime(df['updated_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Populate model
            for _, row in df.iterrows():
                items = []
                for col in df.columns:
                    item = QStandardItem(str(row[col]))
                    item.setEditable(False)
                    items.append(item)
                self.appendRow(items)
                
            logger.info(f"Loaded {len(df)} records into model")
            
        except Exception as e:
            logger.error(f"Error loading data into model: {e}")
            raise

class DatabaseBrowser(QMainWindow):
    """Main window for the database browser application."""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Stock Metrics Database Browser")
        self.setGeometry(100, 100, 1200, 800)
        
        # Create central widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        
        # Create search and filter controls
        controls_layout = QHBoxLayout()
        
        # Search box
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search tickers...")
        self.search_box.textChanged.connect(self.filter_data)
        controls_layout.addWidget(QLabel("Search:"))
        controls_layout.addWidget(self.search_box)
        
        # Column filter
        self.column_filter = QComboBox()
        self.column_filter.addItems([
            "All Columns", "Ticker", "Last Price", "P/E Ratio", "Market Cap"
        ])
        self.column_filter.currentTextChanged.connect(self.filter_data)
        controls_layout.addWidget(QLabel("Filter by:"))
        controls_layout.addWidget(self.column_filter)
        
        # Refresh button
        refresh_btn = QPushButton("Refresh Data")
        refresh_btn.clicked.connect(self.refresh_data)
        controls_layout.addWidget(refresh_btn)
        
        layout.addLayout(controls_layout)
        
        # Create table view
        self.table_view = QTableView()
        self.table_view.setAlternatingRowColors(True)
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.table_view.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table_view)
        
        # Set up model and proxy model for filtering
        self.model = StockMetricsModel()
        self.proxy_model = QSortFilterProxyModel()
        self.proxy_model.setSourceModel(self.model)
        self.table_view.setModel(self.proxy_model)
        
        # Status bar
        self.statusBar().showMessage("Ready")
        self.update_status()
    
    def filter_data(self):
        """Filter the table data based on search text and column selection."""
        search_text = self.search_box.text().lower()
        column = self.column_filter.currentText()
        
        if column == "All Columns":
            self.proxy_model.setFilterKeyColumn(-1)  # Search all columns
        else:
            # Map column name to column index
            column_map = {
                "Ticker": 0,
                "Last Price": 1,
                "P/E Ratio": 5,
                "Market Cap": 10
            }
            self.proxy_model.setFilterKeyColumn(column_map.get(column, -1))
        
        self.proxy_model.setFilterCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.proxy_model.setFilterFixedString(search_text)
        self.update_status()
    
    def refresh_data(self):
        """Refresh the data from the database."""
        try:
            self.model.clear()
            self.model.setHorizontalHeaderLabels([
                "Ticker", "Last Price", "MA100", "EMA100", "% Above MA100",
                "P/E Ratio", "P/B Ratio", "P/S Ratio", "PEG Ratio", "Forward P/E",
                "Market Cap", "Enterprise Value", "EBITDA", "EBITDA/EV",
                "Updated At"
            ])
            self.model.load_data()
            self.statusBar().showMessage("Data refreshed successfully")
            self.update_status()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to refresh data: {str(e)}")
    
    def update_status(self):
        """Update the status bar with current record count."""
        visible_count = self.proxy_model.rowCount()
        total_count = self.model.rowCount()
        self.statusBar().showMessage(f"Showing {visible_count} of {total_count} records")

def main():
    """Main function to run the database browser."""
    try:
        app = QApplication(sys.argv)
        window = DatabaseBrowser()
        window.show()
        sys.exit(app.exec())
    except Exception as e:
        logger.error(f"Error running database browser: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 