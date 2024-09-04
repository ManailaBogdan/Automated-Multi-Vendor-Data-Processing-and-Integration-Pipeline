import signal
import sys
import time

def signal_handler(signum, frame):
    print(f"Received signal: {signum}")
    if signum == signal.SIGINT:
        print("Handling SIGINT (Ctrl+C)")
    elif signum == signal.SIGTERM:
        print("Handling SIGTERM (termination)")
    elif signum == signal.SIGHUP:
        print("Handling SIGHUP (hangup)")
    # Clean up resources if needed
    print("Exiting gracefully")
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)

print("Program is running. Press Ctrl+C to exit.")
while True:
    try:
        # Simulate doing some work
        time.sleep(1)
    except Exception as e:
        print(f"Error: {e}")