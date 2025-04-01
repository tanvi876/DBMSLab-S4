#include "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer(){
    // initialise all blocks as free
    // Stage 6: set other parameters in the metainfo

    // copy blockAllocMap blocks from disk to buffer (using readblock() of disk)
  // blocks 0 to 3
    for(int i=0,blockMapslot=0;i<4;i++){
       unsigned char buffer[BLOCK_SIZE];
       Disk::readBlock(buffer, i);
       for(int slot=0;slot<BLOCK_SIZE;slot++,blockMapslot++){
        StaticBuffer::blockAllocMap[blockMapslot] = buffer[slot];
       }
    }

    for(int bufferIndex = 0; bufferIndex<BUFFER_CAPACITY; bufferIndex++){
        metainfo[bufferIndex].free = true;
        metainfo[bufferIndex].dirty = false;
        metainfo[bufferIndex].blockNum = -1;
        metainfo[bufferIndex].timeStamp = -1;
    }
}

/*
    At this stage, we are not writing back from the buffer to the disk since we are
    not modifying the buffer. So, we will define an empty destructor for now, In
    subsequent stages, we will implement the write-back functionality here.
*/
StaticBuffer::~StaticBuffer(){

    //copy blockALlocMap blocks from buffer to disk
    for(int i=0,blockMapslot=0;i<4;i++){
        unsigned char buffer[BLOCK_SIZE];
        for(int slot=0;slot<BLOCK_SIZE;slot++,blockMapslot++){
         buffer[slot] = blockAllocMap[blockMapslot];
        }
        Disk::writeBlock(buffer, i);
     }
    
    /*
        Iterate through all the buffer block,
        write back blocks with metainfo as free = false, dirty = true using Disk::writeBlock()
    */
    for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++){
        if(metainfo[bufferIndex].free == false 
               && metainfo[bufferIndex].dirty == true){
            Disk::writeBlock(StaticBuffer:: blocks[bufferIndex], metainfo[bufferIndex].blockNum);
        }
    }
}

/*
    Assigns a buffer to the block and returns the buffer number.
    If no free buffer block is found, the least recently used buffer block
    is replaced.
*/
/*
    The timeStamp is reset to 0 each time the buffer block is accessed and
    incremented when other buffer blocks are accessed.
*/
int StaticBuffer::getFreeBuffer(int blockNum){
    if(blockNum < 0 || blockNum >= DISK_BLOCKS){
        return E_OUTOFBOUND;
    }

    int bufferNum = -1, bufferWithMaxTimeStamp = -1, maxTimeStamp = 0;
    // increase the timeStamp in the metaInfo of all occupied buffers.
    // if a free buffer is not avaliable
    //      find the buffer with largest timeStamp
    //      if it is DIRTY, write back to the disk
    //      set bufferNum = index of this buffer
    for(int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++){
        if(metainfo[bufferIndex].free == false){
            metainfo[bufferIndex].timeStamp += 1;

            if(maxTimeStamp < metainfo[bufferIndex].timeStamp){
                bufferWithMaxTimeStamp = bufferIndex;
                maxTimeStamp = metainfo[bufferIndex].timeStamp;
            }
        }

        // if free buffer is avaliable, set bufferNum = bufferIndex
        if(metainfo[bufferIndex].free == true && bufferNum == -1){
            bufferNum = bufferIndex;
        }
    }

    // free buffer not found
    if(bufferNum == -1){
        if(metainfo[bufferWithMaxTimeStamp].dirty == true){
            Disk::writeBlock(StaticBuffer:: blocks[bufferWithMaxTimeStamp], metainfo[bufferWithMaxTimeStamp].blockNum);
        }
        bufferNum = bufferWithMaxTimeStamp;
    }

    // update the metaInfo entry correspinding to bufferNum
    metainfo[bufferNum].free = false;
    metainfo[bufferNum].dirty = false;
    metainfo[bufferNum].blockNum = blockNum;
    metainfo[bufferNum].timeStamp = 0;

    return bufferNum;
}

/*
    Get the buffer index where a particular block is stored
    or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum){
    // check if block is valid (b/w 0 and DISK_BLOCKS)
    if(blockNum < 0 || blockNum >= DISK_BLOCKS){
        return E_OUTOFBOUND;
    }

    // if found, return the bufferIndex which corresponds to blockNum
    for(int i = 0; i < BUFFER_CAPACITY; i++){
        if(metainfo[i].blockNum == blockNum && metainfo[i].free == false){
            return i;
        }
    }

    return E_BLOCKNOTINBUFFER;
}

/*
    setDirtyBit(): sets the dirty bit of the buffer corresponding to the block
*/
int StaticBuffer::setDirtyBit(int blockNum){
    // find the buffer index corresponding to the block using getBufferNum()
    int bufferNum = StaticBuffer::getBufferNum(blockNum);

    if(bufferNum == E_BLOCKNOTINBUFFER){
        return E_BLOCKNOTINBUFFER;
    }

    if(bufferNum == E_OUTOFBOUND){
        return E_OUTOFBOUND;
    }

    // else the buffer is found, set the dirty bit.
    metainfo[bufferNum].dirty = true;
    
    return SUCCESS;
}

int StaticBuffer::getStaticBlockType(int blockNum){
    // Check if blockNum is valid (non zero and less than number of disk blocks)
    // and return E_OUTOFBOUND if not valid.
	if(blockNum<0 or blockNum>=DISK_BLOCKS)
	return E_OUTOFBOUND;

	return (int)blockAllocMap[blockNum];

    // Access the entry in block allocation map corresponding to the blockNum argument
    // and return the block type after type casting to integer.
}