
#include <stdio.h>
#include <stdint.h>

#define VN_HEAD (b) (b >> 6)
#define VN_BODY (b) (b & 0x3F)


struct t_rdb_db_header{
	uint8_t  db_index;

};

struct t_rdb_header{
	uint8_t  sig[5];
	uint8_t  version[4];
};


struct t_small_kv{
	uint8_t  field_type;
	uint32_t expiry_s;
	uint32_t expiry_ms;
	uint32_t key_len;
	uint8_t key[128];
	uint32_t value_len;
	uint8_t value[1024*80];

};

static bool rdb_read_vint(FILE* f, int64_t* out){
	uint8_t tmp[8] = {0};
	uint8_t head_byte = 0;

	*out = 0;
	if (fread(&head_byte, sizeof(uint8_t), 1, f) < 1){
		return false;
	}

	uint8_t h = VN_HEAD(head_byte);
	uint8_t b = VN_BODY(head_byte)
	if (h == 0){
		*out = b;
		return true;
	}

	if (h == 1){
		if (fread(&tmp[0], sizeof(uint8_t), 1, f) < 1){
			return false;
		}
		*out = (((int64_t)b) << 8) + tmp[0];
		return true;
	}

	if (h == 2){
		if (fread(&tmp[0], sizeof(uint8_t), 4, f) < 4){
			return false;
		}
		*out = (((int64_t)4_t)tmp[0]) << 24) + (((int64_t)4_t)tmp[1]) << 16)
			+ (((int64_t)4_t)tmp[2]) << 8) + tmp[3];
		return true;
	}

	printf("ERROR, unknown format: %d-%d\n", (int)h, (int)b);
	return false;
}

static bool rdb_read_key(FILE* f, uint8_t rdbtype, t_small_kv* out){
  switch(rdbtype){
  	case 0: // String
  		break;

  	case 1: // List
  		break;

  	case 2: // Set
  		break;

  	case 0x3: // Sorted Set
  		break;

  	case 0x4: // Hash
  		break;
  	default:
  	    printf("invalid field type:%d\n", flag);
  		return false;
  }
  return true;
}


static void rdb_read_body(FILE* f){
	printf("Begin to read db body\n");

	uint8_t flag = 0;
    struct t_small_kv cur_value;
    while (true) {
        size_t ret = fread(&flag, sizeof(uint8_t), 1, f);
        if (ret < 1) {
            printf("ERROR, invalid field type\n");
        }

        switch (flag) {
        case 0xFF:
                printf("End to read db body\n");
	    		return;
	    	case 0xFE:
	    	    uint8_t db_index = 0;
	    	    if (fread(&db_index, sizeof(uint8_t), 1, f) < 1){
	    	        printf("ERROR, invalid db index\n");
	    	    	return false;
	    	    }
	    		printf("Begin read db: %d\n", db_index);
                continue;
		    	break;

		    case 0xFD:
		    	uint8_t expiry[4];
		    	if (fread(&expiry, sizeof(uint8_t), 4, f) < 4){
	    	        printf("ERROR, invalid expire s\n");
	    	    	return;
	    	    }
                size_t ret = fread(&flag, sizeof(uint8_t), 1, f);
                if (ret < 1) {
                    printf("ERROR, invalid field type\n");
                }
			    break;
		    case 0xFC:
		    	uint8_t expiry[8];
		    	if (fread(&expiry, sizeof(uint8_t), 8, f) < 8){
	    	        printf("ERROR, invalid expire s\n");
	    	    	return false;
	    	    }
                size_t ret = fread(&flag, sizeof(uint8_t), 1, f);
                if (ret < 1) {
                    printf("ERROR, invalid field type\n");
                }
	    	    break;
	    	 
            default:
                rdb_read_key(f, flag, cur_value);
                break;
	    }
	}
}


// fopen(path, O_RDONLY, 0)
void main(int argc, char* argv[]){
	t_rdb_header rh;

    if (argc < 1){
	    printf("ERROR, please input rdb path\n");
    	return;
    }
    const char* file_path = argv[1];
	printf("rdb file path: %s\n", file_path);

    FILE* f = fopen()
	if (fread(&head_byte, sizeof(uint8_t), 1, f) < 1){
		return;
	}

    size_t ret = fread(&rh, sizeof(uint8_t), 9, f);
	if (ret < 9){
		printf("ERROR, invalid rdb file head length: %d\n", ret);
		return false;
	}

}



