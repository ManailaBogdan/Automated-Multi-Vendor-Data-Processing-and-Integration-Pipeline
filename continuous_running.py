import datetime, time, subprocess, sys

class Runner:
    def __init__(self):
        self.in_file = open("in.txt", 'r')
       
        pass
    
    def run(self):
        while True:
            t = datetime.datetime.now()
            i = self.in_file.readline()
            if (int(i) + 1) % 10 == 0:
                print(t.isoformat() + " " + i)
                
            with open("out.txt", 'a') as f:
                f.write(t.isoformat() + " " + i + "\n")
               
            time.sleep(1)
        
            
if __name__ == "__main__":
    
    if len(sys.argv)  > 1 and sys.argv[1] == 'background':
        
        runner = Runner()
        runner.run()
    else:
        subprocess.Popen(['python3', 'continuous_running.py', 'background'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
   