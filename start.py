import time, sys, os, signal, datetime

term = False

def signal_handler(signum, frame):
    if(signum == signal.SIGTERM):
        global term
        print(f"Got {signum}", file=sys.stderr)
        term = True
    
    
signal.signal(signal.SIGTERM, signal_handler)

class CustomError(Exception):
    pass

def main():
    
   

    t = 0
    while True:
        if not term:
            t += 1
            print(t)
            sys.stdout.flush()
            time.sleep(1)
        else:
            print(datetime.datetime.now().isoformat())
            sys.stdout.flush()
            time.sleep(1)
        
        
if __name__ == "__main__":
    main()