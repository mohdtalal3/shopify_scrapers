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
from scrapers_run import run_all_scrapers, run_selected_scrapers, get_available_scrapers
# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for all domains

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Only show INFO and above by default
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Silence noisy libraries
noisy_loggers = ["httpx", "httpcore", "urllib3", "supabase"]
for lib in noisy_loggers:
    logging.getLogger(lib).setLevel(logging.WARNING)

# In-memory store for tracking scraping statu
# In production, you should use Redis or a database
scraping_status = {
    'is_running': False,
    'started_at': None,
    'user_email': None,
    'scraper_ids': None,
    'scraper_count': 0
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
def perform_scraping(user_email, scraper_ids=None):
    """Perform scraping process with optional specific scrapers"""
    global scraping_status
    
    try:
        if scraper_ids:
            logger.info(f"Starting scraping process for user: {user_email}, scrapers: {scraper_ids}")
            results = run_selected_scrapers(scraper_ids)
        else:
            logger.info(f"Starting scraping process for user: {user_email} (all scrapers)")
            results = run_all_scrapers()
        
        # Prepare email body with results
        completed_count = len(results.get('completed', []))
        failed_count = len(results.get('failed', []))
        total_count = results.get('total', 0)
        
        subject = "Shopify Scraping Completed"
        body = f"""
        Hello,
        
        Your Shopify scraping process has been completed.
        
        Started at: {scraping_status['started_at']}
        Completed at: {datetime.now().isoformat()}
        
        Results Summary:
        - Total scrapers: {total_count}
        - Successfully completed: {completed_count}
        - Failed: {failed_count}
        """
        
        if results.get('completed'):
            body += f"\n\nSuccessfully completed scrapers:\n"
            for scraper in results['completed']:
                body += f"✅ {scraper['name']}\n"
        
        if results.get('failed'):
            body += f"\n\nFailed scrapers:\n"
            for scraper in results['failed']:
                body += f"❌ {scraper['name']}: {scraper.get('error', 'Unknown error')}\n"
        
        color_mapping_status = results.get('color_mapping', 'not run')
        body += f"\nColor mapping: {color_mapping_status}"
        
        body += f"""
        
        You can now check your dashboard for the updated data.
        
        Best regards,
        ELYPTRA
        """
        
        send_email(user_email, subject, body, ["eashan.shah@themirage.store", "themirageseo@gmail.com"])
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
        scraping_status['scraper_ids'] = None
        scraping_status['scraper_count'] = 0

@app.route('/api/scrape', methods=['POST'])
def start_scraping():
    """Start the scraping process (all scrapers or selected ones)"""
    global scraping_status
    
    try:
        # Check if scraping is already running
        if scraping_status['is_running']:
            return jsonify({
                'error': 'Another scraping process is already running',
                'started_at': scraping_status['started_at'],
                'user_email': scraping_status['user_email'],
                'scraper_count': scraping_status['scraper_count']
            }), 409
        
        # Get request data
        data = request.get_json()
        user_email = data.get('user_email')
        scraper_ids = data.get('scraper_ids')  # Optional: list of specific scraper IDs
        
        if not user_email:
            return jsonify({'error': 'user_email is required'}), 400
        
        # Validate scraper IDs if provided
        if scraper_ids:
            available_scrapers = get_available_scrapers()
            invalid_ids = [sid for sid in scraper_ids if sid not in available_scrapers]
            if invalid_ids:
                return jsonify({
                    'error': f'Invalid scraper IDs: {invalid_ids}',
                    'available_scrapers': list(available_scrapers.keys())
                }), 400
        
        # Update scraping status
        scraping_status['is_running'] = True
        scraping_status['started_at'] = datetime.now().isoformat()
        scraping_status['user_email'] = user_email
        scraping_status['scraper_ids'] = scraper_ids
        scraping_status['scraper_count'] = len(scraper_ids) if scraper_ids else len(get_available_scrapers())
        
        # Start scraping in background thread
        scraping_thread = threading.Thread(target=perform_scraping, args=(user_email, scraper_ids))
        scraping_thread.daemon = True
        scraping_thread.start()
        
        scraper_type = f"{len(scraper_ids)} selected scrapers" if scraper_ids else "all scrapers"
        logger.info(f"Scraping started for user: {user_email}, type: {scraper_type}")
        
        return jsonify({
            'message': 'Scraping started successfully',
            'user_email': user_email,
            'started_at': scraping_status['started_at'],
            'scraper_count': scraping_status['scraper_count'],
            'scraper_type': scraper_type
        }), 200
        
    except Exception as e:
        logger.error(f"Error starting scraping: {str(e)}")
        
        # Reset status on error
        scraping_status['is_running'] = False
        scraping_status['started_at'] = None
        scraping_status['user_email'] = None
        scraping_status['scraper_ids'] = None
        scraping_status['scraper_count'] = 0
        
        return jsonify({'error': f'Failed to start scraping: {str(e)}'}), 500

@app.route('/api/scrapers', methods=['GET'])
def get_scrapers():
    """Get list of all available scrapers"""
    try:
        available_scrapers = get_available_scrapers()
        scrapers_list = [
            {
                'id': scraper_id,
                'name': scraper_name,
                'display_name': scraper_name
            }
            for scraper_id, (scraper_name, _) in available_scrapers.items()
        ]
        
        return jsonify({
            'scrapers': scrapers_list,
            'total_count': len(scrapers_list)
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting scrapers: {str(e)}")
        return jsonify({'error': f'Failed to get scrapers: {str(e)}'}), 500


@app.route('/api/scrape/selected', methods=['POST'])
def start_selected_scraping():
    """Start scraping for selected scrapers only"""
    global scraping_status
    
    try:
        # Check if scraping is already running
        if scraping_status['is_running']:
            return jsonify({
                'error': 'Another scraping process is already running',
                'started_at': scraping_status['started_at'],
                'user_email': scraping_status['user_email'],
                'scraper_count': scraping_status['scraper_count']
            }), 409
        
        # Get request data
        data = request.get_json()
        user_email = data.get('user_email')
        scraper_ids = data.get('scraper_ids', [])
        
        if not user_email:
            return jsonify({'error': 'user_email is required'}), 400
        
        if not scraper_ids:
            return jsonify({'error': 'scraper_ids is required and must be a non-empty list'}), 400
        
        # Validate scraper IDs
        available_scrapers = get_available_scrapers()
        invalid_ids = [sid for sid in scraper_ids if sid not in available_scrapers]
        if invalid_ids:
            return jsonify({
                'error': f'Invalid scraper IDs: {invalid_ids}',
                'available_scrapers': list(available_scrapers.keys())
            }), 400
        
        # Update scraping status
        scraping_status['is_running'] = True
        scraping_status['started_at'] = datetime.now().isoformat()
        scraping_status['user_email'] = user_email
        scraping_status['scraper_ids'] = scraper_ids
        scraping_status['scraper_count'] = len(scraper_ids)
        
        # Start scraping in background thread
        scraping_thread = threading.Thread(target=perform_scraping, args=(user_email, scraper_ids))
        scraping_thread.daemon = True
        scraping_thread.start()
        
        selected_names = [available_scrapers[sid][0] for sid in scraper_ids]
        logger.info(f"Selected scraping started for user: {user_email}, scrapers: {selected_names}")
        
        return jsonify({
            'message': 'Selected scrapers started successfully',
            'user_email': user_email,
            'started_at': scraping_status['started_at'],
            'scraper_count': len(scraper_ids),
            'selected_scrapers': selected_names
        }), 200
        
    except Exception as e:
        logger.error(f"Error starting selected scraping: {str(e)}")
        
        # Reset status on error
        scraping_status['is_running'] = False
        scraping_status['started_at'] = None
        scraping_status['user_email'] = None
        scraping_status['scraper_ids'] = None
        scraping_status['scraper_count'] = 0
        
        return jsonify({'error': f'Failed to start selected scraping: {str(e)}'}), 500


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
    app.run(debug=False, host="0.0.0.0", port=5000)

