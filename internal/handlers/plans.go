package handlers

import (
	"go-rest-api-kafka/internal/models"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type PlansHandler struct {
	db *gorm.DB
}

func NewPlansHandler(db *gorm.DB) *PlansHandler {
    return &PlansHandler{db: db}
}

func (h *PlansHandler) GetAllPlans(c *gin.Context) {
    var plans []models.Plan
    
    result := h.db.Find(&plans)
    if result.Error != nil {
        c.JSON(http.StatusInternalServerError, gin.H{
            "error": "Failed to fetch plans",
        })
        return
    }

    log.Println(plans)

    c.JSON(http.StatusOK, plans)
}