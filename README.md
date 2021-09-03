#### Node type
| zh name | en name | short | 
| - | - | - | 
| 举报文本 |report text |  RT |
| 举报人 |reporter person |RP |
| 被举报人 |be reproted person |BR |
| 文本关键词 |report text keyword |TK |
| 举报原因 |report reason |RR |

---

#### Edge type
| center node | neigh node | _status |  
| - | - | - | 
| report text | be reported person | 1 |
| reporter person | report text | 1 |
| report text | text keyword | 1 |
| text keyword | report text | 1 |
| report text | report reason | 1 |
| report reason | report text | 1 | 
| be reported person | report person | 1 |
| ... | ... | ...| 
