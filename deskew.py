# deskew an n5 volume
# only deskews top level, pyramid script will do the rest (very quick because of parallelized I/O)

import z5py, sys, numpy as np, itertools,os, time
from common_func import waitOutput

input_name=sys.argv[1]
if not input_name.endswith('/'):
    input_name+='/'

output_name=input_name.split('/')
name=output_name[-2]
output_name='/'.join(output_name[:-2]+[output_name[-2]+'_deskewed','']) # append '_copy' to input directory name

channel=sys.argv[2]

scale='s0' #the other scales are obtained by downsampling
affine_factor=10 # MUST be integer to do shift (should be Z step size/XY pixel size)
chunk_factor=10 # for resampling blocks
spark_nodes=8

base='/groups/svoboda/svobodalab/users/tw/'
scale_py='n5-spark/startup-scripts/spark-janelia/n5-scale-pyramid-nonisotropic.py'

f=z5py.File(input_name)
g=z5py.File(output_name)

if len(sys.argv)==3:
    try:
        group=g.create_group(channel)
        for (x,y) in f.attrs.items():
            g.attrs[x]=y
            print(x,y)
    except Exception as e:
        print(e)
        group=g[channel]
    shape=f[channel][scale].shape
    chunks=f[channel][scale].chunks
    blocksize=chunks[:2]*np.array([1,chunk_factor])
    try:
        group.create_dataset(scale,shape=tuple(map(int,(shape+np.array( (0,0,(shape[0]-1)*affine_factor) )))), chunks=chunks, dtype=f[channel][scale].dtype)
    except Exception as e:
        print(e)
    subgrids = np.ceil(shape[:2]/blocksize).astype(np.int)
    print(scale,np.product(subgrids))
    for i,x in enumerate(itertools.product(*[range(sg) for sg in subgrids])):
        cmd='bsub -o /dev/null -J deskew'+channel+name+'['+str(i+1)+'] python '+os.path.basename(__file__)+' '+input_name+' '+channel+' '+' '.join(map(str,(np.repeat(x,2)+[0,1,0,1])*np.repeat(blocksize,2)))
        print(cmd)
        os.system(cmd)
        time.sleep(0.1)
    waitOutput('deskew'+channel+name,'deskewing')
    
    cmd='python '+base+scale_py+' '+str(spark_nodes)+' -n '+output_name+' -i '+channel+'/'+scale+' -r '+','.join(map(str,f.attrs['pixelResolution']['dimensions']))
    print(cmd)
    os.system(cmd)
else:
    limits=list(map(int,sys.argv[3:]))
    original=f[channel][scale][limits[0]:limits[1],limits[2]:limits[3],:]
    deskewed=np.zeros(original.shape+np.array((0,0,affine_factor*(original.shape[0]-1))))
    for i,x in enumerate(original):
        xstart=(original.shape[0]-i-1)*affine_factor
        deskewed[ i,:, xstart:xstart+original.shape[2] ] = x
    global_x_start=(f[channel][scale].shape[0]-(limits[0]+original.shape[0]))*affine_factor #total height - (local start + local height)
    g[channel][scale][limits[0]:limits[1],limits[2]:limits[3],global_x_start:global_x_start+deskewed.shape[2]]=deskewed
