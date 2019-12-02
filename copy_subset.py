# parallelized copy of full or subset of n5 directory

import z5py, sys, os, time, numpy as np,itertools
from common_func import waitOutput

input_name=sys.argv[1]
if not input_name.endswith('/'):
    input_name+='/'

output_name=input_name.split('/')
name=output_name[-2]
output_name='/'.join(output_name[:-2]+[output_name[-2]+'_copy','']) # append '_copy' to input directory name

scale='s0' #the other scales are obtained by downsampling
spark_nodes=8
channel=sys.argv[2]
f=z5py.File(input_name)
g=z5py.File(output_name)

xy_chunk_factor=10
z_chunk_factor=4

#sample coordinates -- in script not in input..
z0=500
z1=2500
y0=200
y1=3200
x0=28000
x1=31000

shape=f[channel][scale].shape

# for whole volume copy:
#z0,y0,x0=0,0,0
#z1,y1,x1=shape

base='/groups/svoboda/svobodalab/users/tw/'
scale_py='n5-spark/startup-scripts/spark-janelia/n5-scale-pyramid-nonisotropic.py'

subset=np.array([z0,z1,y0,y1,x0,x1]).reshape((3,2)) # Z limit0, Zlimit1, Ylimit0, Ylimit1, Xlimit0, Xlimit1

if len(sys.argv)==3:
    chunks=f[channel][scale].chunks
    blocksize=chunks*np.array([z_chunk_factor,xy_chunk_factor,xy_chunk_factor])
    try:
        group=g.create_group(channel)
        for (x,y) in f.attrs.items():
            g.attrs[x]=y
    except Exception as e:
        print(e)
        group=g[channel]
    try:
        group.create_dataset(scale,shape=list(map(int,np.ceil(np.diff(subset).T[0]/chunks)*chunks)), chunks=chunks, dtype=f[channel][scale].dtype)
    except Exception as e:
        print(e)
    subgrids = np.ceil(np.diff(subset).T/blocksize).astype(np.int)[0]
    print(shape,scale,np.product(subgrids))
    for i,x in enumerate(itertools.product(*[range(sg) for sg in subgrids])):
        cmd='bsub -o /dev/null -J subset'+channel+name+'['+str(i+1)+'] python '+__file__+' '+input_name+' '+channel+' '+' '.join(map(str,(np.repeat(x,2)+[0,1,0,1,0,1])*np.repeat(blocksize,2))) #set limits based on blocksize
        print(cmd)
        os.system(cmd)
        time.sleep(0.05)
    
    waitOutput('subset'+channel+name,'subsetting')
    cmd='python '+base+scale_py+' '+str(spark_nodes)+' -n '+output_name+' -i '+channel+'/'+scale+' -r '+','.join(map(str,f.attrs['pixelResolution']['dimensions']))
    print(cmd)
    os.system(cmd)
else:
    new_limits=list(map(int,sys.argv[3:]))
    new_limits= np.minimum(np.repeat(g[channel][scale].shape,2), new_limits)
    limits=new_limits+np.repeat(subset[:,0],2)
    g[channel][scale][new_limits[0]:new_limits[1],new_limits[2]:new_limits[3],new_limits[4]:new_limits[5]]=f[channel][scale][limits[0]:limits[1],limits[2]:limits[3],limits[4]:limits[5]]