#include <assert.h>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}

// initial list, assign key with k and assign value with v
KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) :
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize,
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique)
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) {
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) {
      return rc;
    }

    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) {
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) {
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;

      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock

  return superblock.Unserialize(buffercache,initblock);
}


ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}


ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value,vector<SIZE_T> &pointer)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);
  SIZE_T rootPtr = superblock.info.rootnode;
  if(node==superblock.info.rootnode)
  {
  pointer.push_back(rootPtr);
  }

  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
        pointer.push_back(ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value,pointer);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) {
      rc=b.GetPtr(b.info.numkeys,ptr);
      pointer.push_back(ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value,pointer);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) {
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) {
	if (op==BTREE_OP_LOOKUP) {
	  return b.GetVal(offset,value);
	}
	else {
	  // BTREE_OP_UPDATE
	  // WRITE ME
    rc = b.SetVal(offset,value);
	if (rc) { return rc; }
	return b.Serialize(buffercache,node);
	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;


  if (dt==BTREE_DEPTH_DOT) {
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) {
      } else {
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) {
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) {
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) {
      if (offset==0) {
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) {
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) {
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) {
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) {
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) {
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) {
    os << "\" ]";
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  vector<SIZE_T> pointer;
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value,pointer);
}


ERROR_T BTreeIndex::insert_not_full_leaf(SIZE_T targetNode,BTreeNode &tempNode,const KEY_T &key,const VALUE_T &value)
{
   ERROR_T rc;
   tempNode.info.numkeys++;
   unsigned int times = 0;
   unsigned int loops = tempNode.info.numkeys;
   
   for (SIZE_T offset = 0; offset < loops - 1; offset++)
   {
   		KEY_T testkey;
   		tempNode.GetKey(offset, testkey);
   		
   		if (key == testkey)
      {
            break;
      }
      else if (key < testkey)
      {
        	for (unsigned int i = tempNode.info.numkeys - 1; i > offset; i--)
        	{
            	KEY_T tempKey;
            	VALUE_T tempVal;
            	tempNode.GetKey(i-1, tempKey);
            	tempNode.GetVal(i-1, tempVal);
            	tempNode.SetKey(i, tempKey);
            	tempNode.SetVal(i, tempVal);
        	}
        	tempNode.SetKey(offset,key);
        	tempNode.SetVal(offset,value);
        	rc = tempNode.Serialize(buffercache, targetNode);
        	if(rc) {return rc;}
        	break;
        }
        else
        {
            times++;
            if (times == (tempNode.info.numkeys -1 ))
            {
            	tempNode.SetKey(tempNode.info.numkeys - 1, key);
            	tempNode.SetVal(tempNode.info.numkeys - 1, value);
            	rc=tempNode.Serialize(buffercache,targetNode);
            	if(rc) {return rc;}
            	break;
            }

        }
   }
}

// insert if the internal node is not full
ERROR_T BTreeIndex::insert_not_full_internal(SIZE_T targetNode,BTreeNode &tempNode,SIZE_T newLeftLeafPtr,SIZE_T newRightLeafPtr,const KEY_T &key)
{
   ERROR_T rc;
   tempNode.info.numkeys++;
   unsigned int loops = tempNode.info.numkeys;
   unsigned int times = 0;
   for (SIZE_T offset = 0;offset < loops - 1; offset++)
   {
   		KEY_T testkey;
   		tempNode.GetKey(offset, testkey);
   		
   		if (key == testkey)
      {
           break;
      }
      else if (key < testkey)
      {
        	for (unsigned int i = tempNode.info.numkeys - 1; i > offset; i--)
        	{	
            	KEY_T tempKey;
            	SIZE_T tempPointer;
           	 	tempNode.GetKey(i - 1, tempKey);
            	tempNode.SetKey(i, tempKey);
           		tempNode.GetPtr(i, tempPointer);
           		tempNode.SetPtr(i + 1, tempPointer);
        	}
        	tempNode.SetKey(offset, key);
        	tempNode.SetPtr(offset, newLeftLeafPtr);
        	tempNode.SetPtr(offset + 1, newRightLeafPtr);
       		rc = tempNode.Serialize(buffercache, targetNode);
        	if(rc) {return rc;}
       		break;
        }
        else
        {
            times++;
            if (times == (tempNode.info.numkeys - 1))
            {
            		tempNode.SetKey(tempNode.info.numkeys - 1, key);
            		tempNode.SetPtr(tempNode.info.numkeys - 1, newLeftLeafPtr);
            		tempNode.SetPtr(tempNode.info.numkeys, newRightLeafPtr);
            		rc = tempNode.Serialize(buffercache, targetNode);
            		if(rc) {return rc;}
            		break;
            }
        }
   }
}


// split the leaf when the leaf is full
ERROR_T BTreeIndex::split_full_leaf(BTreeNode Node,SIZE_T& newLeftLeafPtr,SIZE_T& newRightLeafPtr, KEY_T& Key, VALUE_T& value)
{   
		ERROR_T rc;
		AllocateNode(newLeftLeafPtr);
    AllocateNode(newRightLeafPtr);
    
    // build a new left leaf
    BTreeNode leftLeaf;
    leftLeaf = BTreeNode(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    leftLeaf.Serialize(buffercache, newLeftLeafPtr);
    rc = leftLeaf.Unserialize(buffercache,newLeftLeafPtr);
    if (rc) {return rc;}

		// build a new right leaf
		BTreeNode rightLeaf;
    rightLeaf = BTreeNode(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    rightLeaf.Serialize(buffercache, newRightLeafPtr);
    rc = rightLeaf.Unserialize(buffercache,newRightLeafPtr);
    if (rc) {return rc;}
    
    // move to new left node
    for (unsigned int offset = 0;offset < (Node.info.GetNumSlotsAsLeaf()) / 2; offset++)
    {
        KEY_T tempKey;
        VALUE_T tempValue;
        rc = Node.GetKey(offset, tempKey);
        if (rc){return rc;}
        rc = Node.GetVal(offset, tempValue);
        if(rc){return rc;}
        leftLeaf.info.numkeys++;
        rc = leftLeaf.SetKey(offset, tempKey);
        if(rc) {return rc;}
        rc = leftLeaf.SetVal(offset, tempValue);
        if(rc) {return rc;}
    }
    leftLeaf.Serialize(buffercache,newLeftLeafPtr);

		// move to new right node
    for (unsigned int offset = (Node.info.GetNumSlotsAsLeaf()) / 2; offset < Node.info.GetNumSlotsAsLeaf(); offset++)
    {
        KEY_T tempKey;
        VALUE_T tempValue;
        rc = Node.GetKey(offset, tempKey);
        if (rc){return rc;}
        rc = Node.GetVal(offset, tempValue);
        if(rc){return rc;}
        rightLeaf.info.numkeys++;
        rc = rightLeaf.SetKey(offset - (Node.info.GetNumSlotsAsLeaf()) / 2, tempKey);
        if(rc) {return rc;}
        rc = rightLeaf.SetVal(offset - (Node.info.GetNumSlotsAsLeaf()) / 2, tempValue);
        if(rc) {return rc;}
    }
    rightLeaf.Serialize(buffercache, newRightLeafPtr);
    rc = Node.GetKey((Node.info.GetNumSlotsAsLeaf()) / 2, Key);
    rc = Node.GetVal((Node.info.GetNumSlotsAsLeaf()) / 2, value);
}


// split the internal node
ERROR_T BTreeIndex::split_internal(vector<SIZE_T>& pointer, SIZE_T& newLeftLeafPtr, SIZE_T& newRightLeafPtr, KEY_T& key, int& result)
{
    ERROR_T rc;
    if(result == 0)
    {
    		BTreeNode tempNode;
    		SIZE_T targetNode = pointer.back();
    		pointer.pop_back();
    		rc=tempNode.Unserialize(buffercache,targetNode);
    if(tempNode.info.numkeys == tempNode.info.GetNumSlotsAsInterior() - 1)
    {
        SIZE_T rootPtr = superblock.info.rootnode;
        if(targetNode != rootPtr)
        {
        		SIZE_T newLeftInternalPtr;
       			SIZE_T newRightInternalPtr;
        		KEY_T tempKey;
        		insert_not_full_internal(targetNode, tempNode, newLeftLeafPtr, newRightLeafPtr, key);
        		split_full_internal(tempNode, newLeftInternalPtr, newRightInternalPtr, tempKey);
        		split_internal(pointer, newLeftInternalPtr, newRightInternalPtr, tempKey, result);
        }
        if(targetNode == rootPtr)
        {
        		SIZE_T newLeftInternalPtr;
        		SIZE_T newRightInternalPtr;
        		KEY_T tempKey;
        		BTreeNode newRoot;
        		insert_not_full_internal(targetNode, tempNode, newLeftLeafPtr, newRightLeafPtr, key);
        		split_full_internal(tempNode,newLeftInternalPtr,newRightInternalPtr,tempKey);
        		newRoot=BTreeNode(BTREE_INTERIOR_NODE,superblock.info.keysize,superblock.info.valuesize, superblock.info.blocksize);
        		newRoot.info.numkeys++;
        		newRoot.SetKey(0, tempKey);
        		newRoot.SetPtr(0, newLeftInternalPtr);
        		newRoot.SetPtr(1, newRightInternalPtr);
        		rc = newRoot.Serialize(buffercache, superblock.info.rootnode);
        		result=1;
        		return rc;
        }
    }
    else
    {
        insert_not_full_internal(targetNode, tempNode, newLeftLeafPtr, newRightLeafPtr, key);
        result=1;
        return rc;
    }
    }
    else
    {
    		return rc;
    }
}

ERROR_T BTreeIndex::split_full_internal(BTreeNode Node, SIZE_T& newLeftInternalPtr, SIZE_T& newRightInternalPtr, KEY_T& Key)
{   
		ERROR_T rc;
		AllocateNode(newLeftInternalPtr);
		AllocateNode(newRightInternalPtr);
		
		// build new left internal node
    BTreeNode Left_Internal;
    Left_Internal = BTreeNode(BTREE_INTERIOR_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    Left_Internal.Serialize(buffercache, newLeftInternalPtr);
    rc = Left_Internal.Unserialize(buffercache,newLeftInternalPtr);
    if (rc) {return rc;}
    
    // build new right internal node
    BTreeNode Right_Internal;
    Right_Internal = BTreeNode(BTREE_INTERIOR_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    Right_Internal.Serialize(buffercache, newRightInternalPtr);
    rc = Right_Internal.Unserialize(buffercache,newRightInternalPtr);
    if (rc) {return rc;}
    
    for (unsigned int offset = 0;offset < (Node.info.GetNumSlotsAsInterior()) / 2; offset++)
    {
        KEY_T tempKey;
        SIZE_T tempPointer;
        rc=Node.GetKey(offset,tempKey);
        if (rc){return rc;}
        
        rc=Node.GetPtr(offset,tempPointer);
        Left_Internal.info.numkeys++;
        rc=Left_Internal.SetKey(offset,tempKey);
        rc=Left_Internal.SetPtr(offset,tempPointer);
        if(rc) {return rc;}
    }
        SIZE_T tempPointer;
        rc=Node.GetPtr((Node.info.GetNumSlotsAsInterior()) / 2, tempPointer);
        rc=Left_Internal.SetPtr((Node.info.GetNumSlotsAsInterior()) / 2, tempPointer);
        Left_Internal.Serialize(buffercache, newLeftInternalPtr);

    for (unsigned int offset=(Node.info.GetNumSlotsAsInterior()) / 2 + 1; offset < Node.info.GetNumSlotsAsInterior(); offset++)
    {
        KEY_T tempKey;
        SIZE_T tempPointer;
        rc = Node.GetKey(offset, tempKey);
        if (rc){return rc;}
        rc = Node.GetPtr(offset, tempPointer);
        Right_Internal.info.numkeys++;
        rc = Right_Internal.SetKey(offset - Node.info.GetNumSlotsAsInterior() / 2 - 1, tempKey);
        rc = Right_Internal.SetPtr(offset - Node.info.GetNumSlotsAsInterior() / 2 - 1, tempPointer);
        if(rc) {return rc;}
    }
    SIZE_T tempPoint;
    rc = Node.GetPtr(Node.info.GetNumSlotsAsInterior(), tempPoint);
    rc = Right_Internal.SetPtr((Node.info.GetNumSlotsAsInterior()) / 2 - 1, tempPoint);
    Right_Internal.Serialize(buffercache, newRightInternalPtr);
    rc = Node.GetKey((Node.info.GetNumSlotsAsInterior()) / 2, Key);
}


ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  VALUE_T val;
  bool firstBlock;
  vector<SIZE_T> pointers;
  ERROR_T errorCode = LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, val, pointers);

  switch(errorCode) {
    case ERROR_NOERROR:
    return ERROR_INSERT;

    case ERROR_NONEXISTENT:
    superblock.info.numkeys++;
    BTreeNode leafNode;
    BTreeNode rootNode;
    ERROR_T rc;
    SIZE_T rootPtr = superblock.info.rootnode;
    rootNode.Unserialize(buffercache, rootPtr);
    firstBlock = false;
    if (rootNode.info.numkeys != 0) 
    {
      firstBlock = true;
    }
    rootNode.Serialize(buffercache, rootPtr);
    if(!firstBlock){
      firstBlock = true;
      leafNode = BTreeNode(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
      leafNode.info.numkeys++;
      leafNode.SetKey(0, key);
      leafNode.SetVal(0, value);
      rootNode = leafNode;
      rc = rootNode.Serialize(buffercache, superblock.info.rootnode);
      if (rc) {return rc;}
     }
     if(firstBlock)
     {
        SIZE_T targetNode = pointers.back();
        pointers.pop_back();
        BTreeNode tempNode;
        rc = tempNode.Unserialize(buffercache,targetNode);
        if(tempNode.info.numkeys == tempNode.info.GetNumSlotsAsLeaf() - 1)
        {
        		insert_not_full_leaf(targetNode, tempNode, key, value);
        		SIZE_T newLeftLeafPtr;
        		SIZE_T newRightLeafPtr;
        		SIZE_T rootPtr = superblock.info.rootnode;
        		if(targetNode == rootPtr)
        		{
        				KEY_T tempKey;
        				VALUE_T tempVal;
        				split_full_leaf(tempNode,newLeftLeafPtr,newRightLeafPtr,tempKey,tempVal);
        				BTreeNode newRoot;
        				newRoot = BTreeNode(BTREE_INTERIOR_NODE,superblock.info.keysize,superblock.info.valuesize, superblock.info.blocksize);
        				newRoot.info.numkeys++;
        				newRoot.SetKey(0, tempKey);
        				newRoot.SetPtr(0, newLeftLeafPtr);
        				newRoot.SetPtr(1, newRightLeafPtr);
        				rc = newRoot.Serialize(buffercache, superblock.info.rootnode);
        		}
        		if(targetNode != rootPtr)
        		{
         				KEY_T tempKey;
         				VALUE_T tempVal;
         				int result = 0;
         				split_full_leaf(tempNode,newLeftLeafPtr,newRightLeafPtr,tempKey,tempVal);
         				split_internal(pointers,newLeftLeafPtr,newRightLeafPtr,tempKey,result);
        		}
        }
        else
        {
            insert_not_full_leaf(targetNode,tempNode,key,value);
        }
			}
	}
}


ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
 VALUE_T val = value;
 vector<SIZE_T> pointer;
 return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, val,pointer);
}


ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit
  //
  //
  return ERROR_UNIMPL;
}


//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) {
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);

  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) {
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) {
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) {
      for (offset=0;offset<=b.info.numkeys;offset++) {
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) {
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) {
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) {
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) {
    o << "}\n";
  }
  return ERROR_NOERROR;
}



// keys in every btree node are in order
// the tree is integreted
ERROR_T BTreeIndex::SanityCheck() const
{
	SIZE_T totalKeys = 0;
  ERROR_T errorCode = SanityTraverse(superblock.info.rootnode, totalKeys);
  if (totalKeys != superblock.info.numkeys) 
  {
      return ERROR_INSANE;
  }
	return errorCode;
}


ERROR_T BTreeIndex::SanityTraverse(const SIZE_T &node, SIZE_T &totalKeys) const
{
		ERROR_T rc;
		BTreeNode currentNode;
		SIZE_T offset;
		SIZE_T currentPtr;
		KEY_T testkey;
		KEY_T tempkey;
		VALUE_T value;
		
		rc = currentNode.Unserialize(buffercache, node);
		totalKeys = superblock.info.numkeys;
		//cout<<"switch"<<endl;
		switch(currentNode.info.nodetype){

  		case BTREE_ROOT_NODE:
  		case BTREE_INTERIOR_NODE:
  		//cout<<"currentNode.numkeys: "<<currentNode.info.numkeys<<endl;
  		for(offset = 0; offset < currentNode.info.numkeys; offset++)
  		{
    			rc = currentNode.GetKey(offset, testkey);
    			//if(rc) {return rc;}
					//cout<<"current offset is: "<<offset<<endl;
    			if(offset + 1 < currentNode.info.numkeys - 1)
    			{
      				rc = currentNode.GetKey(offset + 1, tempkey);
      				if(tempkey < testkey)
      				{
        					cout<<"The keys are not in order."<<endl;
      				}
      				//else
      				//{
      						//cout<<"The keys are stored in order"<<endl;
      				//}
    			}
    			rc = currentNode.GetPtr(offset, currentPtr);
    			//if(rc){return rc;}
    			return SanityTraverse(currentPtr, totalKeys);
  		}
  		if(currentNode.info.numkeys > 0)
  		{
    			rc = currentNode.GetPtr(currentNode.info.numkeys, currentPtr);
    			//if(rc) { return rc; }
      		return SanityTraverse(currentPtr, totalKeys);
  		}
  		else
  		{
    			cout << "The keys is nonexistent."<<endl;
    			return ERROR_NONEXISTENT;
  		}
  		break;
  		case BTREE_LEAF_NODE:
  		//cout<<"leaf node"<<endl;
  		for(offset = 0; offset < currentNode.info.numkeys; offset++)
  		{
    			rc = currentNode.GetKey(offset, testkey);
    			if(rc) 
    			{
      				cout << "missing key"<<endl;
      				return rc;
    			}
    			//else
    			//{
    					//cout<<"no missing key"<<endl;
    			//}
    			rc = currentNode.GetVal(offset, value);
    			if(rc)
    			{
      				cout << "missing value"<<endl;
      				return rc;
    			}
    			//else
    			//{
    					//cout<<"no missing value"<<endl;
    			//}
    			if(offset + 1 < currentNode.info.numkeys)
    			{
      				rc = currentNode.GetKey(offset + 1, tempkey);
      				if(tempkey < testkey)
      				{
        					cout<<"The keys are not in order."<<endl;
      				}
    			}
  			}
  			break;
  			default:
  				return ERROR_INSANE;
  			break;
			}
			return ERROR_NOERROR;
		}


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  ERROR_T rc;
  rc = Display(os, BTREE_DEPTH_DOT);
  return os;
}



