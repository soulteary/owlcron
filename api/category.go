package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/soulteary/owlcron/types"
)

// handleCreateCategory 处理创建分类
func (s *Server) handleCreateCategory(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	var category types.Category
	if err := json.NewDecoder(r.Body).Decode(&category); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}

	if !s.authorizeCategoryMutation(w, r, category.ID) {
		return
	}
	if category.ID == "" {
		category.ID = generateID()
	}

	if category.Title == "" {
		s.sendError(w, 1, "category title is required")
		return
	}

	if err := ValidateCategory(&category); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	exists, err := s.engine.GetStorage().Exists(r.Context(), "categories:"+category.ID)
	if err != nil {
		s.sendError(w, 1, "Failed to check category: "+err.Error())
		return
	}
	if exists {
		s.sendError(w, 1, "Category already exists")
		return
	}
	// 保存到存储
	if err := s.engine.SaveCategoryToStorage(&category); err != nil {
		s.sendError(w, 1, "Failed to save category: "+err.Error())
		return
	}

	s.configurationChanged("categories")
	s.sendResponse(w, 0, map[string]interface{}{"id": category.ID}, "")
}

// handleUpdateCategory 处理更新分类
func (s *Server) handleUpdateCategory(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	var category types.Category
	if _, err := s.decodeStoredPatch(r, "categories", &category); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}

	if !s.authorizeCategoryMutation(w, r, category.ID) {
		return
	}
	// 验证分类数据
	if err := ValidateCategory(&category); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}

	// 更新到存储
	if err := s.engine.SaveCategoryToStorage(&category); err != nil {
		s.sendError(w, 1, "Failed to update category: "+err.Error())
		return
	}

	s.configurationChanged("categories")
	s.sendResponse(w, 0, nil, "")
}

// handleDeleteCategory 处理删除分类
func (s *Server) handleDeleteCategory(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method != http.MethodPost {
		s.sendError(w, 1, "Method not allowed")
		return
	}

	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, 1, "Invalid request body: "+err.Error())
		return
	}

	if !s.authorizeCategoryMutation(w, r, req.ID) {
		return
	}
	if s.configurationInUse("categories", req.ID) {
		s.sendError(w, 1, "Category is still assigned to events")
		return
	}
	if err := s.deleteConfigurationRecord(r, "categories", req.ID); err != nil {
		s.sendError(w, 1, err.Error())
		return
	}
	s.configurationChanged("categories")

	s.sendResponse(w, 0, nil, "")
}

// handleGetCategory 处理获取分类
func (s *Server) handleGetCategory(w http.ResponseWriter, r *http.Request) {
	categoryID := readRequestID(r)
	if categoryID == "" {
		s.sendError(w, 1, "Missing category id")
		return
	}

	// 从存储获取分类
	ctx := context.Background()
	storage := s.engine.GetStorage()
	if storage == nil {
		s.sendError(w, 1, "Storage not initialized")
		return
	}
	key := fmt.Sprintf("categories:%s", categoryID)
	data, err := storage.Get(ctx, key)
	if err != nil {
		s.sendError(w, 1, "Category not found")
		return
	}

	var category types.Category
	if err := json.Unmarshal(data, &category); err != nil {
		s.sendError(w, 1, "Failed to parse category: "+err.Error())
		return
	}

	s.sendResponse(w, 0, map[string]interface{}{"category": &category}, "")
}

// handleListCategories 处理列出分类
func (s *Server) handleListCategories(w http.ResponseWriter, r *http.Request) {
	// 从存储获取所有分类
	categoriesMap, err := s.engine.LoadCategoriesFromStorage()
	if err != nil {
		s.sendError(w, 1, "Failed to load categories: "+err.Error())
		return
	}

	categories := make([]*types.Category, 0, len(categoriesMap))
	for _, category := range categoriesMap {
		auth := GetAuthContext(r)
		if auth == nil || (!auth.Privileges["admin"] && auth.Privileges["cat_limit"] && !auth.Privileges["cat_"+category.ID]) {
			continue
		}
		categories = append(categories, category)
	}

	s.sendResponse(w, 0, recordPage(r, categories, func(record *types.Category) string { return record.ID }), "")
}

// Validate the decoded body's ID as well as middleware parameters: a query
// parameter must never authorize a different object than the mutation body.
func (s *Server) authorizeCategoryMutation(w http.ResponseWriter, r *http.Request, id string) bool {
	auth := GetAuthContext(r)
	if auth == nil {
		s.sendError(w, 1, "Authentication required")
		return false
	}
	if !auth.IsAdmin && auth.Privileges["cat_limit"] && (id == "" || !auth.Privileges["cat_"+id]) {
		s.sendError(w, 1, "Category access denied")
		return false
	}
	return true
}
