from flask import Flask, request, jsonify
from flask_cors import CORS
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import threading
import time
from datetime import datetime
import logging
from dotenv import load_dotenv
from scrapers_run import run_all_scrapers
# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for all domains

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In-memory store for tracking scraping status
# In production, you should use Redis or a database
scraping_status = {
    'is_running': False,
    'started_at': None,
    'user_email': None
}

def send_email(to_email, subject, body, cc_emails=None):
    """Send email notification with optional CC"""
    try:
        smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', '587'))
        sender_email = os.getenv('FROM_EMAIL')
        sender_password = os.getenv('EMAIL_PASSWORD')

        if not sender_email or not sender_password:
            logger.error("Email credentials not configured")
            return False

        # Ensure cc_emails is a list
        cc_emails = cc_emails or []
        # Combine recipients for sending
        recipients = [to_email] + cc_emails

        # Create message
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = to_email
        if cc_emails:
            message["Cc"] = ", ".join(cc_emails)
        message["Subject"] = subject

        # Add body
        message.attach(MIMEText(body, "plain"))

        # SMTP session
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipients, message.as_string())
        server.quit()

        logger.info(f"Email sent successfully to {recipients}")
        return True

    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        return False


        # Started at: {scraping_status['started_at']}
        # 
def perform_scraping(user_email):
    """Simulate scraping process"""
    global scraping_status
    
    try:
        logger.info(f"Starting scraping process for user: {user_email}")
        run_all_scrapers()
        subject = "Shopify Scraping Completed"
        body = f"""
        Hello,
        
        Your Shopify scraping process has been completed successfully.
        Started at: {scraping_status['started_at']}
        Completed at: {datetime.now().isoformat()}
        
        You can now check your dashboard for the updated data.
        
        Best regards,
        ELYPTRA
        """
        
        send_email(user_email, subject, body,["eashan.shah@themirage.store","themirageseo@gmail.com"])
        logger.info(f"Scraping completed for user: {user_email}")
        
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        
        # Send error email
        subject = "Shopify Scraping Failed"
        body = f"""
        Hello,
        
        Unfortunately, your Shopify scraping process encountered an error and could not be completed.
        
        Error: {str(e)}
        Started at: {scraping_status['started_at']}
        Failed at: {datetime.now().isoformat()}
        
        Please try again or contact support if the issue persists.
        
        Best regards,
        ELYPTRA
        """
        
        send_email(user_email, subject, body)
        
    finally:
        # Reset scraping status
        scraping_status['is_running'] = False
        scraping_status['started_at'] = None
        scraping_status['user_email'] = None

@app.route('/api/scrape', methods=['POST'])
def start_scraping():
    """Start the scraping process"""
    global scraping_status
    
    try:
        # Check if scraping is already running
        if scraping_status['is_running']:
            return jsonify({
                'error': 'Another scraping process is already running',
                'started_at': scraping_status['started_at'],
                'user_email': scraping_status['user_email']
            }), 409
        
        # Get request data
        data = request.get_json()
        user_email = data.get('user_email')
        
        if not user_email:
            return jsonify({'error': 'user_email is required'}), 400
        
        # Update scraping status
        scraping_status['is_running'] = True
        scraping_status['started_at'] = datetime.now().isoformat()
        scraping_status['user_email'] = user_email
        
        # Start scraping in background thread
        scraping_thread = threading.Thread(target=perform_scraping, args=(user_email,))
        scraping_thread.daemon = True
        scraping_thread.start()
        
        logger.info(f"Scraping started for user: {user_email}")
        
        return jsonify({
            'message': 'Scraping started successfully',
            'user_email': user_email,
            'started_at': scraping_status['started_at']
        }), 200
        
    except Exception as e:
        logger.error(f"Error starting scraping: {str(e)}")
        
        # Reset status on error
        scraping_status['is_running'] = False
        scraping_status['started_at'] = None
        scraping_status['user_email'] = None
        
        return jsonify({'error': f'Failed to start scraping: {str(e)}'}), 500

@app.route('/api/scrape/status', methods=['GET'])
def get_scraping_status():
    """Get current scraping status"""
    return jsonify(scraping_status), 200

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()}), 200

if __name__ == '__main__':
    # Development server
    #app.run(debug=True, host='0.0.0.0', port=5002)
    app.run(ssl_context=("cert.pem", "key.pem"), host="0.0.0.0", port=5000)

