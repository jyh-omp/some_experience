package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"os"
	"strings"
	"time"
)

type tParti struct {
	ShareId int64 `db:"id"`
	UserId  int64 `db:"user_id"`
}

func main() {

	/*读取t_xxx聚合表中的user_id作为t_xxxx_paticipate表中的邀请者写入link字段*/

	dataType := 1
	DB := 0

	connStr := fmt.Sprintf("xxxx:xxxxxx@(host_ip:port)/db_%d", DB)
	db, sqlErr := sqlx.Connect("mysql", connStr)
	if sqlErr != nil {
		fmt.Println("mannul connect failed.", sqlErr)
		os.Exit(1)
	}
	juheDb, juheSqlErr := sqlx.Connect("mysql", "user_xx:password_xx@(host_ip:port)/db_name")
	if juheSqlErr != nil {
		fmt.Println("juhe database connect failed.")
		os.Exit(2)
	}

	for i := 32 * DB; i < 32*(DB+1); i++ {
		userIdSql := fmt.Sprintf("select share_id from t_xxxxx_participate_%d where type = %d and share_id > 0 and link is null", i, dataType)
		fmt.Println(userIdSql)
		shareIds := make([]int64, 0)
		db.Select(&shareIds, userIdSql)
		fmt.Println("get user_id num:", len(shareIds))

		//批量查询数据，然后批量更新
		for k := 0; k < len(shareIds); k += 100 {
			var max int
			if k+100 > len(shareIds) {
				max = len(shareIds)
			} else {
				max = k + 100
			}
			tShareIds := shareIds[k:max]
			tStr := strings.Replace(strings.Trim(fmt.Sprint(tShareIds), "[]"), " ", ",", -1)

			tSql := fmt.Sprintf("select id, user_id from t_share where id in (%s)", tStr)
			//fmt.Println(tSql)
			structRecs := make([]tParti, 0)
			juheDb.Select(&structRecs, tSql)

			//批量更新
			updateSql := fmt.Sprintf("update t_share_participate_%d set link = case share_id ", i)
			var whenSql string
			for _, rec := range structRecs {
				whenSql += fmt.Sprintf("when %d then %d ", rec.ShareId, rec.UserId)
			}
			updateSql += whenSql
			updateSql += fmt.Sprintf("end where share_id in (%s)", tStr)

			//fmt.Println(updateSql)
			db.Exec(updateSql)

			if k%1000 == 0 {
				fmt.Println(k, "done.")
				time.Sleep(300 * time.Millisecond)
			}
		}
		fmt.Printf("t_share_participate_%d done.\n", i)
	}

}
