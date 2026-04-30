package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
	kamacache "github.com/youngyangyang04/KamaCache-Go"
)

type AdminController struct {
	server *kamacache.Server
	picker *kamacache.ClientPicker
}

type cacheRequest struct {
	Group string `json:"group"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type nodeRequest struct {
	Addr string `json:"addr"`
}

func NewAdminController(server *kamacache.Server, picker *kamacache.ClientPicker) *AdminController {
	return &AdminController{
		server: server,
		picker: picker,
	}
}

func (a *AdminController) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
	})
}

func (a *AdminController) Nodes(c *gin.Context) {
	response := gin.H{}
	if a.server != nil {
		response["server"] = a.server.Snapshot()
	}
	if a.picker != nil {
		response["picker"] = a.picker.Snapshot()
	}
	response["groups"] = groupSnapshots()
	c.JSON(http.StatusOK, response)
}

func (a *AdminController) AddNode(c *gin.Context) {
	if a.picker == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "peer picker is not configured"})
		return
	}

	var req nodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := a.picker.AddPeer(req.Addr); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "node added",
		"addr":    req.Addr,
		"picker":  a.picker.Snapshot(),
	})
}

func (a *AdminController) DeleteNode(c *gin.Context) {
	if a.picker == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "peer picker is not configured"})
		return
	}

	addr := c.Query("addr")
	if addr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "addr is required"})
		return
	}

	if err := a.picker.RemovePeer(addr); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "node removed",
		"addr":    addr,
		"picker":  a.picker.Snapshot(),
	})
}

func (a *AdminController) HashRing(c *gin.Context) {
	if a.picker == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "peer picker is not configured"})
		return
	}
	snapshot := a.picker.Snapshot()
	c.JSON(http.StatusOK, gin.H{
		"ring":              snapshot.Ring,
		"old_ring":          snapshot.OldRing,
		"old_ring_saved_at": snapshot.OldRingSavedAt,
	})
}

func (a *AdminController) Stats(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"server": serverSnapshot(a.server),
		"picker": pickerSnapshot(a.picker),
		"groups": groupSnapshots(),
	})
}

func (a *AdminController) Query(c *gin.Context) {
	groupName := firstNonEmpty(c.Query("group"), "test")
	key := c.Query("key")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}

	group := kamacache.GetGroup(groupName)
	if group == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	view, err := group.Get(c.Request.Context(), key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"group": groupName,
		"key":   key,
		"value": view.String(),
		"stats": group.Stats(),
	})
}

func (a *AdminController) Set(c *gin.Context) {
	var req cacheRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	req.Group = firstNonEmpty(req.Group, "test")
	if req.Key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}

	group := kamacache.GetGroup(req.Group)
	if group == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	if err := group.Set(c.Request.Context(), req.Key, []byte(req.Value)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"group": req.Group,
		"key":   req.Key,
		"value": req.Value,
		"stats": group.Stats(),
	})
}

func (a *AdminController) Delete(c *gin.Context) {
	groupName := firstNonEmpty(c.Query("group"), "test")
	key := c.Query("key")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key is required"})
		return
	}

	group := kamacache.GetGroup(groupName)
	if group == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "group not found"})
		return
	}

	if err := group.Delete(c.Request.Context(), key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"group":   groupName,
		"key":     key,
		"deleted": true,
		"stats":   group.Stats(),
	})
}

func groupSnapshots() map[string]map[string]interface{} {
	groups := make(map[string]map[string]interface{})
	for _, name := range kamacache.ListGroups() {
		group := kamacache.GetGroup(name)
		if group != nil {
			groups[name] = group.Stats()
		}
	}
	return groups
}

func serverSnapshot(server *kamacache.Server) interface{} {
	if server == nil {
		return nil
	}
	return server.Snapshot()
}

func pickerSnapshot(picker *kamacache.ClientPicker) interface{} {
	if picker == nil {
		return nil
	}
	return picker.Snapshot()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
