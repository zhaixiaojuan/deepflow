package service

import "github.com/deepflowys/deepflow/server/controller/db/mysql"

func GetVPCs(filter map[string]interface{}) ([]*mysql.VPC, error) {
	db := mysql.Db
	if _, ok := filter["name"]; ok {
		db = db.Where("name = ?", filter["name"])
	}
	var vpcs []*mysql.VPC
	if err := db.Order("created_at DESC").Find(&vpcs).Error; err != nil {
		return nil, err
	}
	return vpcs, nil
}
