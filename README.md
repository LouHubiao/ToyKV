## 目前支持的类型：

定长：

 Int, long,char；（未来支持double）

不定长：

 不可变长：Cell, struct,

可变长：string, list

## 节点结构：

定长：

只存储其内容。

不定长：

Status表示数据当前的状态，Length表示存储数据的长度（byte为单位）， nextPart此数据的后面部分，nextNode表示后一个节点(偏移量)，preNode表示前一个节点(偏移量)，body表示数据具体内容（注意： **使用偏移量而不直接存储内存地址，不定长数据必须放在同一个memory block中** ）。内存结构如下：

memory struct:

    string:     status(8)| byteLength(16)| Body| [curCount(16)]| [nextPart(32)]|

    list:       status(8)| byteLength(16)| Body| [curCount(16)]| [nextPart(32)]|

    struct:     status(8)| Body|

    cell:       status(8)| Body|

    deleted:    status(8)| byteLength(16)| [freeNext(32)]| [freePre(32)]| ...|

    status:     isDeleted| hasNext| isFull| IsLocked|...

IsDeleted表示此节点内容是否被删除，在数据被删除后，每隔一段时间，内存会进行碎片整理。

HasNext表示此数据是否有后续部分，string和list需要使用此字段，在update和insert的操作中，可能会产生不连续的数据。Struct和cell此字段一直为0；

IsLocked表示此数据是否正在被修改（包括增加、删除、修改）。有四种状态：00 空闲，01被一个线程占用，02 多个线程在进行使用，03 有线程正在进行操作。在写数据的过程中，如果是00那么占用此节点；如果是01那么将状态改为02，然后睡眠一段时间；如果是02那么释放线程占用的其他资源；如果是03那么此线程等待。

## 索引结构：

使用B树进行索引。支持多种key类型，object需要设置campare方法。
对于string类型，使用了Trie Tree，节约内存容量

## 内存管理：

 首先申请一片连续内存块，并设置为手动释放模式，如果在使用过程中内存地址超出此内存块最大限制， 则申请新的一片内存块。

 所有定长结构使用原始数据进行存储，增加修改都直接对内存进行修改。所有不定长结构都抽象为一个内存 **偏移地址** ，地址指向具体存储的起始地址。插入不定长结构会存储在目前内存块的最末端。最终形成连续的存储空间（增加cache命中率）。

### 全局数据：

List&lt;IntPtr&gt; memAddrs用于存储每一个内存块的起始地址；

List&lt;int&gt; memCounts用于存储每一个内存块的cell数量；

List&lt;IntPtr[]&gt; freeAdds用于存储空闲内存链表；

List&lt;IntPtr&gt; tailAddrs用于存储当前最尾部内存地址（插入新数据的位置）

Int16 gap用于可变长度变量的缓冲冗余空间

### Insert：

对于不定长数据的插入。首先确定其长度，然后判断是否存在大小合适的空闲内存，如果不存在，则插入到tailAddr后，如果存在，则从空闲链表中卸下对应内存。

如果插入List或者string或者struct，则同时需要插入指针；如果是cell类型，则需要修改附近cells的nextNode和preNode；如果在插入listPart过程中与后面数据冲突，则修改hasNext并且增加nextPart指针。

对于可变长度类型（string和list），需要用户提前设置相邻两个数据之间的gap，给未来修改数据进行缓冲。同时影响isFull。

### Delete：

对于list中定长数据的删除，将list最后的数据放置插入到删除数据的位置，修改length大小。

对于不定长数据的删除。通过标记节点中IsDeleted来进行删除，而不修改索引树的内容，如果是list或者struct或者string类型，则同时需要删除对应的指针；如果是cell类型，则修改preNode-&gt;nextNode=cur-&gt;nextNode。在删除后，将空闲的内存空间挂在freeAdds下。

### Update：

对于不定长数据的Update操作（只有string可以update），如果修改后长度小于之前的长度，可以直接进行修改，反之则转化为先deleted然后再insert。

## 代码结构

    Main{
        初始化，
        自定义函数(统计)，
        TX{
            操作代码(自动生成),
            MemHelper(内存块内管理),
            BlockHelper(内存块管理)
        }
    }

## 举例：

GE结构：

    struct In
    {
            CellId tx_index;        //发起方比特币来源
            string addr;                //发起方地址
    }
    cellstruct Tx
    {
            long time;                //支付发生的时间
            string hash;                //支付的64位hash值
            List<In>; ins;                //支付发起方信息
            List<string> outs;        //支付接收方地址列表
            long amount;                //支付金额
    }

    cell User
    {
        List<long> txs;     //某个地址相关的所有支付记录
    }

ToyGE结构：

    Tx {
        status      byte
        CellID      Int64
        hash        int32   // =>hash
        time        Int64
        ins         int32   // =>ins
        outs        int32   // =>outs
        amount      Int64
    }
    hash{
        status      byte
        length      int16
        context     byte[]
        [curLnegth] int32
        [nextPart]  int32
    }
    ins{
        status      byte
        length      int16
        context     int32[] //=>in
        [curLnegth] int32
        [nextPart]  int32
    }
    in{
        status      byte
        addr        int32   // =>in_addr
        tx_index    Int64
    }
    in_addr{
        status      byte
        length      int16
        context     byte[]
        [curLnegth] int32
        [nextPart]  int32
    }
    outs{
        status      byte
        length      int16
        context     int32[] //=>out
        [curLnegth] int32
        [nextPart]  int32

    }
    out{
        status      byte
        length      int16
        context     byte[]
        [curLnegth] int32
        [nextPart]  int32
    }

    User {
        status      byte
        nextNode    int32   // next node
        preNode     int32   // pre node
        txs         int32   // =>txs
    }
    txs{
        status      byte
        length      int16
        cellID      Int64[]
        [curLnegth] int32
        [nextPart]  int32
    }

插入数据举例：

{&quot;CellID&quot;:15029, &quot;hash&quot;:&quot;f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16&quot;, &quot;time&quot;:1231731025, &quot;ins&quot;:[{&quot;addr&quot;:&quot;12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S&quot;, &quot;tx\_index&quot;:14862}, &quot;addr&quot;:&quot;1LzBzVqEeuQyjD2mRWHes3dgWrT9titxvq&quot;, &quot;tx\_index&quot;:15045}], &quot;outs&quot;:[&quot;1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5Jvm3&quot;,&quot;12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S&quot;], &quot;amount&quot;:1000000000}

测试结果：

    GE:
        只用Tx，占用内存4.1G，耗时2分钟
        Tx+User，占用4.6G，耗时2分钟
    ToyGE:
        只用Tx，占用2.6G，耗时2分钟
        Tx+User，占用3.6G，耗时4分钟

    索引测试：100000000 节点
    BTree       耗时4分35s，内存
    TrieTree    耗时10分，内存1G，搜索时间

其他：

1.为了尽可能提高内存利用率，对于可变长类型，使用了三种状态：notFull，isFull，hasnext。其中isFull内存利用率最高，也是最可能出现的情况（申请多少用多少）。其他两种在数据的最后有4个字节的冗余。对最大长度限制为64KB。

2.删除数据后需要对空闲数据尽可能merge，但是必须在O(1)时间内完成，所以使用了向后merge的策略，也防止merge过度导致小块内存缺失。

3.如果删除的内存块体积无法放入nextFree和preFree，则不放入这些冗余数据，作为孤立的空闲内存，等待自动回收内存。（自动回收未完成）

4.在Update和Insert过程中，会递归下降的进行插入操作，当某一部分无法获得空闲内存时，之前插入的数据会算作已经插入成功的部分。如果当前block没有多余的空闲内存，则返回修改内存失败，会一直返回到cell层，由CellHelper寻求新的空闲空间，将整个cell移动到新的内存地址后，再执行更新内存操作，并更新索引树。（未完成）

5.list的set与add，保持isFull=1，length会随着数据的插入而增加，如果一个item插入不成功，则返回失败，整个cell会搜索新的空闲空间并重新插入。

5.对于不同的schema，需要并发执行Load。（未完成）