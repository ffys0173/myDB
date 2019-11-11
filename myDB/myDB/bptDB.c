#include"bptDB.h"

// 퀵소트로 바꿀것...
void sort(page_t* page) {
	int i, j;
	uint64_t tmp_key = 0;
	uint64_t tmp_pn = 0;
	char tmp_val[120] = "";

	//리프 노드
	if (page->node.is_leaf == TRUE) {

		for (i = 0; i < page->node.num_of_keys - 1; i++)
		{
			for (j = 0; j < page->node.num_of_keys - 1 - i; j++)
			{
				if (page->node.data.leaf[j].key > page->node.data.leaf[j + 1].key)
				{
					tmp_key = page->node.data.leaf[j].key;
					strcpy(tmp_val, page->node.data.leaf[j].value);

					page->node.data.leaf[j].key = page->node.data.leaf[j + 1].key;
					strcpy(page->node.data.leaf[j].value, page->node.data.leaf[j + 1].value);

					page->node.data.leaf[j + 1].key = tmp_key;
					strcpy(page->node.data.leaf[j + 1].value, tmp_val);
				}
			}
		}
	}
	// 인터널 노드
	else if (page->node.is_leaf == FALSE){

		for (i = 0; i < page->node.num_of_keys - 1; i++)
		{
			for (j = 0; j < page->node.num_of_keys - 1 - i; j++)
			{
				if (page->node.data.internal[j].key > page->node.data.internal[j + 1].key)
				{
					tmp_key = page->node.data.internal[j].key;
					tmp_pn = page->node.data.internal[j].page_number;

					page->node.data.internal[j].key = page->node.data.internal[j + 1].key;
					page->node.data.internal[j].page_number = page->node.data.internal[j + 1].page_number;

					page->node.data.internal[j + 1].key = tmp_key;
					page->node.data.internal[j + 1].page_number = tmp_pn;
				}
			}
		}
	}
}

int insert_into_internal(pagenum_t old_num, uint64_t key, pagenum_t value) {

	page_t old;
	page_t tmp;
	page_t new_internal;
	pagenum_t new_internal_num = file_alloc_page();
	file_read_page(old_num, &old);

	// 부모노드(인터널)가 오더를 초과한 경우 분할
	if (old.node.num_of_keys == INTERNAL_ORDER) {

		new_internal_num = file_alloc_page();
		
		memset(&new_internal, 0, PAGE_SIZE);
		new_internal.node.parent_page_number = old.node.parent_page_number;
		new_internal.node.sibling_pn = old_num;

		data_split(&old, &new_internal);
		file_write_page(new_internal_num, &new_internal);
		insert_into_internal(new_internal.node.parent_page_number, key, new_internal_num);

		// old에서 분리된 노드의 부모를 new로 지정
		for (int i = 0; i < new_internal.node.num_of_keys; i++) {
			file_read_page(new_internal.node.data.internal[i].page_number, &tmp);
			tmp.node.parent_page_number = new_internal_num;
			file_write_page(new_internal.node.data.internal[i].page_number, &tmp);
		}

		// old랑 new 중 어디에 넣을지...
		if (new_internal.node.data.internal[0].key > key) { // new로 넣는 경우

			new_internal.node.data.internal[new_internal.node.num_of_keys].key = key;
			new_internal.node.data.internal[new_internal.node.num_of_keys].page_number = value;
			new_internal.node.num_of_keys++;

			sort(&new_internal);
			file_write_page(new_internal_num, &new_internal);
		}

		else { // old로 넣는 경우
			old.node.data.internal[old.node.num_of_keys].key = key;
			old.node.data.internal[old.node.num_of_keys].page_number = value;
			old.node.num_of_keys++;

			sort(&old);
			file_write_page(old_num, &old);
		}
	}

	else { // 일반적으로 삽입하는 경우
		old.node.data.internal[old.node.num_of_keys].key = key;
		old.node.data.internal[old.node.num_of_keys].page_number = value;
		old.node.num_of_keys++;

		sort(&old);
		file_write_page(old_num, &old);
	}

	return 0;
}

int data_split(page_t* oldp, page_t* newp) {

	int flag, i;

	flag = (oldp->node.num_of_keys / 2) + 1;

	if (oldp->node.is_leaf == TRUE && newp->node.is_leaf == TRUE) {

		for (i = 0; i < flag; i++) {
			newp->node.data.leaf[i].key = oldp->node.data.leaf[oldp->node.num_of_keys - flag + i].key;
			strcpy(newp->node.data.leaf[i].value, oldp->node.data.leaf[oldp->node.num_of_keys - flag + i].value);

			oldp->node.data.leaf[oldp->node.num_of_keys - flag + i].key = 0;
			memset(&oldp->node.data.leaf[oldp->node.num_of_keys - flag + i].value, 0, 120);
		}

		newp->node.num_of_keys = oldp->node.num_of_keys - flag + 1;
		oldp->node.num_of_keys = flag - 1;

		return 0;
	}

	else if (oldp->node.is_leaf == FALSE && newp->node.is_leaf == FALSE) {

		for (i = 0; i < flag; i++) {
			newp->node.data.internal[i].key = oldp->node.data.internal[oldp->node.num_of_keys - flag + i].key;
			newp->node.data.internal[i].page_number = oldp->node.data.internal[oldp->node.num_of_keys - flag + i].page_number;

			oldp->node.data.internal[oldp->node.num_of_keys - flag + i].key = 0;
			oldp->node.data.internal[oldp->node.num_of_keys - flag + i].page_number = 0;
		}

		newp->node.num_of_keys = oldp->node.num_of_keys - flag + 1;
		oldp->node.num_of_keys = flag - 1;

		return 0;
	}

	else { // 이 루틴엔 진입할 수 없음... 만일 진입하면 ㅈ된거
		return -1;
	}
}

int leaf_split(page_t* old, pagenum_t old_num) {

	page_t parent;
	page_t newp;
	pagenum_t parent_num;
	pagenum_t newp_num;
	pagenum_t old_parent_num = old->node.parent_page_number;
	int flag, result = 0;

	// old가 리프노드인지 조사
	if (old->node.is_leaf == FALSE) {
		return -1;
	}

	// 부모 노드 조사
	if (old->node.parent_page_number == 0) { // 부모노드가 존재하지 않는다면(헤더였다면) 새로 생성 후 번호 부여, 초기화
		parent_num = file_alloc_page();
		memset(&parent, 0, PAGE_SIZE);
		header_page.header.root_page_number = parent_num;
		parent.node.sibling_pn = old_num;
		parent.node.parent_page_number = old_parent_num;
		file_write_page(parent_num, &parent);
	}

	else { // 부모노드가 존재하는 경우 parent에 읽기
		parent_num = old->node.parent_page_number;
		file_read_page(parent_num, &parent);
	}

	// 분할에 의한 새로운 노드에 번호 부여
	newp_num = file_alloc_page();
	memset(&newp, 0, PAGE_SIZE);
	newp.node.is_leaf = TRUE;

	// internal로 삽입
	flag = (old->node.num_of_keys / 2);
	result = insert_into_internal(parent_num, old->node.data.leaf[flag].key, newp_num);

	// 데이터 분리
	data_split(old, &newp);

	// 연결
	old->node.sibling_pn = newp_num;
	old->node.parent_page_number = parent_num;
	newp.node.parent_page_number = parent_num;
	
	//파일 쓰기
	file_write_page(0, &header_page);
	file_write_page(old_num, old);
	file_write_page(newp_num, &newp);

	return 0;
}

pagenum_t find_node(uint64_t key) {

	int i;
	page_t tmp;
	uint64_t cur_page_number = header_page.header.root_page_number;

	if (cur_page_number == 0) {
		return 0;
	}

	while (TRUE) {
		file_read_page(cur_page_number, &tmp);

		// reaf node를 만난 경우
		if (tmp.node.is_leaf == TRUE) {
			break;
		}

		// 저장된 키값 중 가장 작은 값보다 더 작은 입력이 들어온 경우
		if (tmp.node.data.internal[0].key > key) {
			cur_page_number = tmp.node.sibling_pn;
			continue;
		}
		
		for (i = 0; i < tmp.node.num_of_keys; i++) {
					
			if (tmp.node.data.internal[i].key > key) { // i = 0일 때에는 이 조건에 걸릴 수 없음. 위 if문에서 continue되기 때문.
				cur_page_number = tmp.node.data.internal[i - 1].page_number;
				break;
			}

			else if (tmp.node.data.internal[i].key <= key) {
				if (i + 1 == tmp.node.num_of_keys) {
					cur_page_number = tmp.node.data.internal[i].page_number;
					break;
				}
				if(tmp.node.data.internal[i + 1].key > key) {
					cur_page_number = tmp.node.data.internal[i].page_number;
					break;
				}
			}
		}
	}

	return cur_page_number; // exist or to be inserted
}

int db_find(uint64_t key, char* ret_val) {

	pagenum_t result = find_node(key);
	page_t tmp;

	if (result == 0) {
		return -2; // leaf page not exist 이거 필요한가?
	}

	file_read_page(result, &tmp);
	for (int i = 0; i < tmp.node.num_of_keys; i++) {
		if (tmp.node.data.leaf[i].key == key) {
			strcpy(ret_val, tmp.node.data.leaf[i].value);
			return 0; // found
		}
	}
	return -1; //not found
}

pagenum_t file_alloc_page() {
	
	pagenum_t new_page_num = header_page.header.number_of_pages++;
	file_write_page(0, &header_page);
	return new_page_num;
}

void file_free_page(pagenum_t pagenum) {

}

void file_read_page(pagenum_t pagenum, page_t* dest) {
	memset(dest, 0, PAGE_SIZE);
	fseek(fp, pagenum * PAGE_SIZE, SEEK_SET);
	fread(dest, PAGE_SIZE, 1, fp);
}

void file_write_page(pagenum_t pagenum, const page_t* src) {
	fseek(fp, pagenum * PAGE_SIZE, SEEK_SET);
	fwrite(src, PAGE_SIZE, 1, fp);
}

void init_bpt() {

	//page_t* free_page = (page_t*)malloc(PAGE_SIZE * 99);

	memset(&header_page, 0, PAGE_SIZE);
	file_read_page(0, &header_page);

	/*memset(free_page, 0, PAGE_SIZE * 99);
	fseek(fp, PAGE_SIZE, SEEK_SET);
	fwrite(free_page, PAGE_SIZE, 99, fp);*/

	if (header_page.header.number_of_pages == NULL) {
		printf("헤더 페이지 생성.\n");
		header_page.header.number_of_pages = 1;
		header_page.header.root_page_number = 0;
		header_page.header.free_page_number = 100;
		fseek(fp, 0, SEEK_SET);
		fwrite(&header_page, PAGE_SIZE, 1, fp);
	}

	else {
		printf("헤더 페이지 존재\n");
		printf("루트 페이지 넘버 : %ld\n", (long)header_page.header.root_page_number);
		printf("할당된 페이지 개수 : %ld\n", (long)header_page.header.number_of_pages);
		printf("미할당 페이지 개수 : %ld\n", (long)header_page.header.free_page_number);
	}
}

int open_table(char* pathname) {

	int size;
	int init_done = FALSE;
	fp = fopen(pathname, "r+b");

	if (fp == NULL) {
		fp = fopen(pathname, "wb");
		init_bpt();
		init_done = TRUE;
		fclose(fp);

		fp = fopen(pathname, "r+b");
	}

	if (!init_done) {
		init_bpt();
		init_done = TRUE;
	}

	// 파일 사이즈가 4096의 배수인지 체크
	fseek(fp, 0, SEEK_END);
	size = ftell(fp);
	if (size % PAGE_SIZE != 0)
		return -1;

	return 0;
}

int db_insert(uint64_t key, char* value) {

	int is_first = FALSE;
	page_t page;
	pagenum_t page_num = find_node(key);

	// 루트페이지가 존재하지 않는 경우, 첫 인서트의 경우
	if (page_num == 0) { 
		page_num = file_alloc_page();
		header_page.header.root_page_number = 1;
		file_write_page(0, &header_page);
		is_first = TRUE;
	}

	// 이하 통상적인 경우
	file_read_page(page_num, &page);
	page.node.is_leaf = TRUE; // finde_node는 무조건 leaf노드를 반환, leaf 노드가 없던 경우엔 위 코드에 의해 생성
	
	// 루트페이지를 생성한 경우 헤더를 부모로 지정
	if (is_first) {
		page.node.parent_page_number = 0;
	}

	// 동일한 키 값을 가진 데이터가 존재하는 경우
	for (int i = 0; i < page.node.num_of_keys; i++) {
		if (page.node.data.leaf[i].key == key) {
			printf("인서트 실패. 동일한 키 값을 가진 데이터가 존재함.\n");
			return -1;
		}
	}

	// 한 노드의 최대 크기를 초과하는 경우
	if (page.node.num_of_keys >= LEAF_ORDER) {
		leaf_split(&page, page_num);
		db_insert(key, value);
		return 0;
	}

	// 데이터를 삽입할 수 있는 경우
	else {
		page.node.data.leaf[page.node.num_of_keys].key = key;
		strcpy(page.node.data.leaf[page.node.num_of_keys].value, value);
		page.node.num_of_keys++;
		sort(&page);
		file_write_page(page_num, &page);
	}

	return 0;
}

void show_all_pages(void){

	page_t tmp;
	for (int i = 1; i < header_page.header.number_of_pages; i++) {

		file_read_page(i, &tmp);
		
		if (tmp.node.is_leaf == TRUE) {
			/*printf("\npage number : %d\n", i);
			printf("number of keys : %d\n", tmp.node.num_of_keys);
			printf("parent page : %ld\n", (long)tmp.node.parent_page_number);
			printf("sibling page : %ld\n", (long)tmp.node.sibling_pn);
			for (int j = 0; j < tmp.node.num_of_keys; j++) {
				printf("key : %ld value : %s\n", (long)tmp.node.data.leaf[j].key, tmp.node.data.leaf[j].value);
			}*/
		}

		else if (tmp.node.is_leaf == FALSE) {
			printf("\nInternal page\n");
			printf("page number : %d\n", i);
			printf("number of keys : %d\n", tmp.node.num_of_keys);
			printf("parent page : %ld\n", (long)tmp.node.parent_page_number);
			printf("sibling page : %ld\n", (long)tmp.node.sibling_pn);
			for (int j = 0; j < tmp.node.num_of_keys; j++) {
				printf("key : %ld page number : %ld\n", (long)tmp.node.data.internal[j].key, (long)tmp.node.data.internal[j].page_number);
			}
		}
	}
}

int main()
{
	int i;
	char* path = "test.dat";

	if (open_table(path) == -1) {
		printf("테이블을 읽을 수 없습니다.\n");
		return 0;
	}

	printf("size of single page : %lld\n", sizeof(page_t));

	//for (i = 0; i < 7800; i++) {
	//	int result = db_insert(i, "hello world");
	//	if(result == 0)
	//		printf("%d 인서트 성공!\n", i);
	//}

	show_all_pages();

	fclose(fp);

	return 0;
}