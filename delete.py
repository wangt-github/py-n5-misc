# parallelized remove of n5 directory

import sys, os, time, random
from common_func import waitOutput

def delete(dir):
    cmd='bsub -o /dev/null -J '+job_ID+' rm -rf '+dir
    print(cmd)
    os.system(cmd)
    time.sleep(0.05)

input_name=sys.argv[1]
if not input_name.endswith('/'):
    input_name+='/'

if not os.path.isdir(input_name) or 'attributes.json' not in list(os.listdir(input_name)):
    print(input_name,'is NOT an N5 directory! Be careful!!!')
    time.sleep(5)
    quit()

job_ID='remove'+str(int(random.random()*10000))

for f in os.listdir(input_name):
    if os.path.isdir(input_name+f) and f.startswith('c'):
        for g in os.listdir(input_name+f+'/'):
            if g=='s0' or g=='s1':
                for h in os.listdir(input_name+f+'/'+g+'/'):
                    delete(input_name+f+'/'+g+'/'+h)
            else:
                delete(input_name+f+'/'+g)

waitOutput(job_ID,'deletions',5)
cmd='rm -rf '+input_name
print(cmd)
os.system(cmd)
