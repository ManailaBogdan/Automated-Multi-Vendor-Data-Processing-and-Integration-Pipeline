import subprocess

p = subprocess.Popen(['ls', '-l'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

while True:
    out, _ = p.communicate()
    print(out)
    print("..")