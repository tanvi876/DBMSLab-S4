#include "StaticBuffer.h"
#include <cstring>
unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];


//modifications to print number of comparisons
int StaticBuffer::numCompares = 0;

StaticBuffer::StaticBuffer() {

    for (int i = 0; i < 4; i++) {
        unsigned char buffer[BLOCK_SIZE];
        Disk::readBlock(buffer, i);
        memcpy(blockAllocMap + i*BLOCK_SIZE, buffer, BLOCK_SIZE);
    }


    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        metainfo[i].free = true;
        metainfo[i].dirty = false;
        metainfo[i].timeStamp = -1;
        metainfo[i].blockNum = -1;
    }
}

StaticBuffer::~StaticBuffer() {

    for (int i = 0; i < 4; i++) {
        unsigned char buffer[BLOCK_SIZE];
        memcpy(buffer, blockAllocMap + i*BLOCK_SIZE, BLOCK_SIZE);
        Disk::writeBlock(buffer, i);
    }


    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (!metainfo[i].free && metainfo[i].dirty) 
            Disk::writeBlock(StaticBuffer::blocks[i], metainfo[i].blockNum);
    }
}

int StaticBuffer::getFreeBuffer(int blockNum) {
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
        return E_OUTOFBOUND;

    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (!metainfo[i].free)
            metainfo[i].timeStamp++;
    }




    int allocatedBuffer = -1;

    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (metainfo[i].free) {
            allocatedBuffer = i;
            break;
        }
    }

    if (allocatedBuffer == -1) {
        int highestTimeStamp = 0;
        for (int i = 0; i < BUFFER_CAPACITY; i++) {
            if (metainfo[i].timeStamp > highestTimeStamp) {
                highestTimeStamp = metainfo[i].timeStamp;
                allocatedBuffer = i;
            }
        }

        if (metainfo[allocatedBuffer].dirty)
            Disk::writeBlock(StaticBuffer::blocks[allocatedBuffer], metainfo[allocatedBuffer].blockNum);
    }

    metainfo[allocatedBuffer].free = false;
    metainfo[allocatedBuffer].dirty = false;
    metainfo[allocatedBuffer].blockNum = blockNum;
    metainfo[allocatedBuffer].timeStamp = 0;

    return allocatedBuffer;
}

int StaticBuffer::getBufferNum(int blockNum) {
    if (blockNum < 0 || blockNum > DISK_BLOCKS)
        return E_OUTOFBOUND;
    
    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (metainfo[i].blockNum == blockNum)
            return i;
    }

    return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum) {
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
        return E_OUTOFBOUND;


    int targetBuffer = -1;
    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (!metainfo[i].free && metainfo[i].blockNum == blockNum) {
            targetBuffer = i;
            break;
        }
    }

    if (targetBuffer == -1)
        return E_BLOCKNOTINBUFFER;

    metainfo[targetBuffer].dirty = true;

    return SUCCESS;
}


int StaticBuffer::getStaticBlockType(int blockNum) {
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
        return E_OUTOFBOUND;

    unsigned char blockType = blockAllocMap[blockNum];

    return (int)blockType;
}