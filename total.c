/*************************************************************************
    > File Name: tatol.c
    > Author: gaopeng QQ:22389860 all right reserved
    > Mail: gaopp_200217@163.com
    > Created Time: Fri 10 Mar 2017 04:20:32 PM CST
 ************************************************************************/

#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<stdint.h>
#include<time.h>

#define ERR_POST __FILE__,__LINE__

typedef struct node_p
{
	struct node_p* next;
	void* data;
} NODE_P;


typedef struct head_p
{
	NODE_P* head;
	NODE_P* end;
} HEAD_P;

 typedef struct op_str
 {
	 char db_name[512];
	 char tab_name[512];
	 uint64_t	ino;
	 uint64_t	inoc;
	 uint64_t	upo;
	 uint64_t	upoc;
	 uint64_t	deo;
	 uint64_t	deoc;
	 uint64_t	cnt;
	 uint64_t	cntc;
 } OP_STR;



 HEAD_P MTR_POS_H ;//MAX TRX SIZE HEAD
 HEAD_P TIM_POS_H ;//TIME PIECE HEAD


 uint64_t POS_T = 0;
 uint64_t POS_E = 0;
 uint64_t TRX_TOTAL = 0;
 uint64_t EVE_TOTAL = 0 ;
 uint64_t MAX_EVE = 0;
 uint64_t MAX_EVE_P = 0;
 uint64_t PI_SIZE = 0;
 uint32_t POS_TIME = 0;
 uint32_t END_TIME = 0;
 uint32_t BEG_TIME = 0;
 uint64_t MAX_FILE_Z = 0;

 double MAX_TRX_CUT = 0;
 //time support 0929
 static unsigned char       TIME_BUF[80];

//add new 2017 08 12

uint32_t QUR_T = 0 ;
uint32_t QUR_X = 0 ; 

uint32_t QUR_POS = 0;
uint32_t QUR_POS_END = 0;

uint32_t EXE_T = 0;



static unsigned char* utime_local(time_t tim)
{
    struct tm  ts;
    char       buf[80];

	memset(TIME_BUF,0,sizeof(buf));

    // Format time, "yyyy-mm-dd hh:mm:ss(zzz)"
    ts = *localtime(&tim);
	
    strftime(TIME_BUF, sizeof(TIME_BUF), "%Y%m%d %H:%M:%S(%Z)", &ts);   
	return TIME_BUF;
}






int freechain(HEAD_P* head_p);


int init_chain(uint64_t a,uint64_t b,HEAD_P* head)
{
	int ret = 0;
	uint64_t* trx_ar = NULL ;
	NODE_P* node = NULL;
	
	if ( (trx_ar = (uint64_t*)calloc(2,sizeof(uint64_t))) == NULL)
	{
		ret = 1;
		printf("init_chain:MEM calloc ERROR %s %d \n",ERR_POST);
		goto err;
	}

	if((node = (NODE_P*)calloc(1,sizeof(NODE_P))) ==NULL)
	{
		ret = 2;
		printf("init_chain:MEM calloc %s %d \n",ERR_POST);
		goto err;
		
	}

	*trx_ar = a; //end pos
	*(trx_ar+1) = b; //begin pos
	
	node->data = (void*)trx_ar;
	
	if(head->head == NULL)
	{
		head->head = node;
		head->end = node;
		node->next = NULL;
	}
	else
	{
		head->end->next = node;
		head->end = node;
		node->next = NULL;
	}

	return ret;
	
err:
	
	if(trx_ar != NULL)
	{
		free(trx_ar);
		trx_ar = NULL;
	}
	if(node != NULL)
	{
		free(node);
		node = NULL;
	}
	return ret;
}


int init_chain2(uint64_t a,uint64_t b,uint64_t c,uint64_t d,uint64_t e,HEAD_P* head)
{
	int ret = 0;
	uint64_t* trx_ar = NULL ;
	NODE_P* node = NULL;
	
	if ( (trx_ar = (uint64_t*)calloc(5,sizeof(uint64_t))) == NULL)
	{
		ret = 1;
		printf("init_chain:MEM calloc ERROR %s %d \n",ERR_POST);
		goto err;
	}

	if((node = (NODE_P*)calloc(1,sizeof(NODE_P))) ==NULL)
	{
		ret = 2;
		printf("init_chain:MEM calloc %s %d \n",ERR_POST);
		goto err;
		
	}

	*trx_ar = a; //end time
	*(trx_ar+1) = b; //begin time
	*(trx_ar+2) = c; //query pos
	*(trx_ar+3) = d; //query end
	*(trx_ar+4) = e; //query exe_time
	
	node->data = (void*)trx_ar;
	
	if(head->head == NULL)
	{
		head->head = node;
		head->end = node;
		node->next = NULL;
	}
	else
	{
		head->end->next = node;
		head->end = node;
		node->next = NULL;
	}

	return ret;
	
err:
	
	if(trx_ar != NULL)
	{
		free(trx_ar);
		trx_ar = NULL;
	}
	if(node != NULL)
	{
		free(node);
		node = NULL;
	}
	return ret;
}

/* 0 no node find 1 node find and add values */
int find_add_chain_op(const char* db_name,const char* table_name,uint64_t ino,uint64_t upo,uint64_t deo,uint64_t inoc,uint64_t upoc,uint64_t deoc, HEAD_P* head)
{
  NODE_P* node_pi = NULL;
  OP_STR* data = NULL;
	if (db_name == NULL||table_name == NULL )
	{
			printf("db_name == NULL||table_name == NULL is True %s %d \n",ERR_POST);
			exit(-1);
	}
		
		if(head->head == NULL)
		{
			return 0;
		}
		node_pi = head->head;
		while(node_pi)
		{
			data=(OP_STR*)(node_pi->data);			
			if(!strcmp(db_name,data->db_name) && !strcmp(table_name,data->tab_name) )
			{
					data->ino = data->ino+ino;
					data->upo = data->upo+upo;
					data->deo = data->deo+deo;
					data->inoc = data->inoc+inoc;
					data->upoc = data->upoc+upoc;
					data->deoc = data->deoc+deoc;
					return 1;
			}			
			node_pi = node_pi->next;
		}
	return 0;//not find node
}





int add_chain_op(const char* db_name,const char* table_name,uint64_t ino,uint64_t upo,uint64_t deo, uint64_t inoc,uint64_t upoc,uint64_t deoc, HEAD_P* head)
{
	int ret = 0;
	OP_STR* data = NULL ;
	NODE_P* node = NULL;
	
	if (db_name == NULL||table_name == NULL )
	{
			ret = 3;
			printf("db_name == NULL||table_name == NULL is True %s %d \n",ERR_POST);
			goto err;
	}
	
	if(!(find_add_chain_op(db_name,table_name,ino,upo,deo,inoc,upoc,deoc,head))) //如果没有找到需要新建
	{
	    if ( (data = (OP_STR*)calloc(1,sizeof(OP_STR))) == NULL)
	    {
	    	ret = 1;
	    	printf("init_chain:MEM calloc ERROR %s %d \n",ERR_POST);
	    	goto err;
	    }
	    if((node = (NODE_P*)calloc(1,sizeof(NODE_P))) ==NULL)
	    {
	    	ret = 2;
	    	printf("init_chain:MEM calloc %s %d \n",ERR_POST);
	    	goto err;
	    }
	    
	    strcpy(data->db_name,db_name);
	    strcpy(data->tab_name,table_name);
	    data->ino = ino;
	    data->upo = upo;
	    data->deo = deo;
		data->inoc = inoc;
		data->upoc = upoc;
		data->deoc = deoc;
		
	    node->data = (void*)data;
	    if(head->head == NULL)
	    {
	    	head->head = node;
	    	head->end = node;
	    	node->next = NULL;
	    }
	    else
	    {
	    	head->end->next = node;
	    	head->end = node;
	    	node->next = NULL;
	    }
	}

	return ret;

err:
	
	if(data != NULL)
	{
		free(data);
		data = NULL;
	}
	if(node != NULL)
	{
		free(node);
		node = NULL;
	}
	return ret;
	    
}


int add_and_change(NODE_P* before,NODE_P* after)
{
	OP_STR* be = (OP_STR*)before->data;
	OP_STR* af = (OP_STR*)after->data;
	
	OP_STR* tmp =  (OP_STR*)calloc(1,sizeof(OP_STR));
	
	be->cnt = be->ino+be->upo+be->deo;
	be->cntc = be->inoc+be->upoc+be->deoc;
	af->cnt = af->ino+af->upo+af->deo;
	af->cntc = af->inoc+af->upoc+af->deoc;
	if(be->cnt > af->cnt)
	{
			memcpy(tmp,be,sizeof(OP_STR));
			memcpy(be,af,sizeof(OP_STR));
			memcpy(af,tmp,sizeof(OP_STR));
	}	
	free(tmp);
	return 0;
}



  
int BubbleSort(HEAD_P* head,int count)
{   
    NODE_P* pMove;  
    pMove = head->head;  
  
	if(count == 1) //bug fixed 如果count = 1 缺失统计
	{
			OP_STR* be = (OP_STR*)pMove->data;
	        be->cnt = be->ino+be->upo+be->deo;
	        be->cntc = be->inoc+be->upoc+be->deoc;
			return 0;
	}
	 //遍历次数为count-1 
    while (count > 1) 
    {  
        while (pMove->next) 
        {  
			add_and_change(pMove,pMove->next);
            pMove = pMove->next;  
        }  
        count--;  
        //重新移动到第一个节点  
        pMove = head->head;
    }  
    return 0;
}  



int print_total(HEAD_P* head_pi,HEAD_P* head_trx,HEAD_P* head_mt ,HEAD_P* head_tab,uint64_t mi,uint32_t mt)
{
	int ret = 0;
	uint64_t be_t = 0;
	uint64_t en_t = 0;
	NODE_P* node_pi = NULL;
	NODE_P* node_trx = NULL;
	uint64_t trx_b = 0;
	uint64_t trx_e = 0;
	uint64_t i = 0;
	
	uint64_t trx_qb = 0;
	uint64_t trx_qe = 0;
    uint64_t trx_qpos = 0;
	uint64_t trx_qpos_e = 0;
	

	printf("-------------Total now--------------\n");
	printf("Trx total[counts]:%ld\n",TRX_TOTAL);
	printf("Event total[counts]:%ld\n",EVE_TOTAL);
	printf("Max trx event size:%ld(bytes) Pos:%ld[0X%lX]\n",MAX_EVE,MAX_EVE_P,MAX_EVE_P);
	printf("Avg binlog size(/sec):%.3f(bytes)[%.3f(kb)]\n",(float)MAX_FILE_Z/(float)(END_TIME-BEG_TIME),(float)MAX_FILE_Z/(float)(END_TIME-BEG_TIME)/1024);
	printf("Avg binlog size(/min):%.3f(bytes)[%.3f(kb)]\n",(float)MAX_FILE_Z/(float)(END_TIME-BEG_TIME)*60,(float)MAX_FILE_Z/(float)(END_TIME-BEG_TIME)*60/1024);
	printf("--Piece view:\n");
	
    if(head_pi->head != NULL)
	{
		i = 1;
		node_pi = head_pi->head;
		while(node_pi)
		{
			if(node_pi->next != NULL)
			{
				be_t =  *((uint64_t*)(node_pi->data)+1);
				en_t =  *((uint64_t*)(node_pi->next->data)+1);
				printf("(%ld)Time:%ld-%ld(%ld(s)) piece:%ld(bytes)[%.3f(kb)]\n",i,be_t,en_t,en_t-be_t,*((uint64_t*)(node_pi->data)),(float)(*((uint64_t*)(node_pi->data)))/(float)1024 );
			}
			if(node_pi->next == NULL)
			{
				be_t =  *((uint64_t*)(node_pi->data)+1);
				en_t =  (uint64_t)END_TIME;
				printf("(%ld)Time:%ld-%ld(%ld(s)) piece:%ld(bytes)[%.3f(kb)]\n",i,be_t,en_t,en_t-be_t,*((uint64_t*)(node_pi->data)),(float)(*((uint64_t*)(node_pi->data)))/(float)1024);
			}

			node_pi = node_pi->next;
			i++;
		}
	}
	else
	{
		ret = 2;
		printf("print_total: no piece? %s %d \n",ERR_POST);
		goto err;

	}
	printf("--Large than %ld(bytes) trx:\n",mi);
	if(head_trx->head != NULL)
	{
		i = 1;
		node_trx = head_trx->head;
		while(node_trx)
		{
			trx_b = *((uint64_t*)(node_trx->data)+1);
			trx_e = *((uint64_t*)(node_trx->data));
			printf("(%ld)Trx_size:%ld(bytes)[%.3f(kb)] trx_begin_p:%ld[0X%lX] trx_end_p:%ld[0X%lX]\n",i,trx_e-trx_b,(float)(trx_e-trx_b)/(float)1024,trx_b,trx_b,trx_e,trx_e);
			node_trx = node_trx->next;
			i++;
			MAX_TRX_CUT += (double)((float)(trx_e-trx_b)/(float)1024) ;
		}
		printf("Total large trx count size(kb):#%.3f(kb)\n",MAX_TRX_CUT);

	}
	else
	{
		printf("No trx find!\n");
	}

	printf("--Large than %u(secs) trx:\n",mt);
	if(head_mt->head != NULL)
	{
		i = 1;
		node_trx = head_mt->head;
		while(node_trx)
		{
			trx_qb = *((uint64_t*)(node_trx->data)+1);
			trx_qe = *((uint64_t*)(node_trx->data));
			trx_qpos = *((uint64_t*)(node_trx->data)+2);
			trx_qpos_e = *((uint64_t*)(node_trx->data)+3);
			/*time */
			unsigned char trx_bt[80];
			unsigned char trx_et[80];

			strcpy(trx_bt,utime_local(trx_qb));
			strcpy(trx_et,utime_local(trx_qe));
			
			
			printf("(%ld)Trx_sec:%ld(sec)  trx_begin_time:[%s] trx_end_time:[%s] trx_begin_pos:%ld trx_end_pos:%ld query_exe_time:%ld \n",i,trx_qe-trx_qb,trx_bt,trx_et,trx_qpos,trx_qpos_e, *((uint64_t*)(node_trx->data)+4));
			node_trx = node_trx->next;
			i++;
		}

	}
	else
	{
		printf("No trx find!\n");
	}
	printf("--Every Table binlog size(bytes) and times:\n");
	i = 0;
	
	if(head_tab->head != NULL)
	{
		node_trx = head_tab->head;
		while(node_trx)
		{
		  node_trx = node_trx->next;
		  i++;
		}
	}

	if(head_tab->head != NULL)
	{
		//进行冒泡的排序
		BubbleSort(head_tab,i);
	}
	
	if(head_tab->head != NULL)
	{
		uint64_t end_total = 0;
		uint64_t end_cntc = 0;
		i = 1;
		node_trx = head_tab->head;
		printf("Note:size unit is bytes\n");
		while(node_trx)
		{
			//printf("(%ld)Current Table:%s.%s Insert:size(%ld(Bytes)) times(%ld) Update:size(%ld(Bytes)) times(%ld) Delete:size(%ld(Bytes)) times(%ld) Total:size(%ld(Bytes)) times(%ld)\n",i,((OP_STR*)(node_trx->data))->db_name,((OP_STR*)(node_trx->data))->tab_name,((OP_STR*)(node_trx->data))->ino,((OP_STR*)(node_trx->data))->inoc, ((OP_STR*)(node_trx->data))->upo, ((OP_STR*)(node_trx->data))->upoc ,((OP_STR*)(node_trx->data))->deo,((OP_STR*)(node_trx->data))->deoc,((OP_STR*)(node_trx->data))->cnt,((OP_STR*)(node_trx->data))->cntc);
			printf("---(%ld)Current Table:%s.%s::\n",i,((OP_STR*)(node_trx->data))->db_name,((OP_STR*)(node_trx->data))->tab_name);
			printf("   Insert:binlog size(%ld(Bytes)) times(%ld)\n",((OP_STR*)(node_trx->data))->ino,((OP_STR*)(node_trx->data))->inoc);
			printf("   Update:binlog size(%ld(Bytes)) times(%ld)\n",((OP_STR*)(node_trx->data))->upo, ((OP_STR*)(node_trx->data))->upoc);
			printf("   Delete:binlog size(%ld(Bytes)) times(%ld)\n",((OP_STR*)(node_trx->data))->deo,((OP_STR*)(node_trx->data))->deoc);
			printf("   Total:binlog size(%ld(Bytes)) times(%ld)\n",((OP_STR*)(node_trx->data))->cnt,((OP_STR*)(node_trx->data))->cntc);

			end_total += ((OP_STR*)(node_trx->data))->cnt;
			end_cntc += ((OP_STR*)(node_trx->data))->cntc;
			node_trx = node_trx->next;
			i++;
		}
		printf("---Total binlog dml event size:%ld(Bytes) times(%ld)\n",end_total,end_cntc);
	}
	else
	{
		printf("No DML BINLOG find!\n");
	}	
			
	freechain(head_pi);
	freechain(head_trx);
	freechain(head_mt);
	freechain(head_tab);

	return ret;
err:
	freechain(head_pi);
	freechain(head_trx);
	freechain(head_mt);
	freechain(head_tab);
	return ret;
}


int freechain(HEAD_P* head_p)
{
	int ret = 0;

	NODE_P* node_n = NULL;
	NODE_P* node_p = NULL;

	if(head_p != NULL)
	{
		if(head_p->head != NULL )
		{
			node_p = head_p->head;
			while(node_p)
			{
				node_n = node_p ; //save current node
				node_p = node_p->next; //next node
				free(node_n ->data);
				free(node_n);
				node_n = NULL;
			}
			head_p->head = NULL;
			head_p->end = NULL;
		}
	}
	return ret;
}

