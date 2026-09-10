use super::*;

impl MVCCEngine {
    // --- View Management Methods ---

    /// Create a new view
    pub fn view_exists(&self, name: &str) -> Result<bool> {
        self.view_exists_lowercase(&name.to_lowercase())
    }

    /// Check if a view exists (assumes name is already lowercase)
    /// Use this when you already have a lowercase name to avoid allocation
    #[inline]
    pub fn view_exists_lowercase(&self, name_lower: &str) -> Result<bool> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let views = self.views.read().unwrap();
        Ok(views.contains_key(name_lower))
    }

    /// Get a view definition
    pub fn get_view(&self, name: &str) -> Result<Option<Arc<ViewDefinition>>> {
        self.get_view_lowercase(&name.to_lowercase())
    }

    /// Get a view definition (assumes name is already lowercase)
    /// Use this when you already have a lowercase name to avoid allocation.
    /// Returns Arc clone (cheap pointer copy, no data clone).
    #[inline]
    pub fn get_view_lowercase(&self, name_lower: &str) -> Result<Option<Arc<ViewDefinition>>> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let views = self.views.read().unwrap();
        Ok(views.get(name_lower).cloned()) // Arc::clone is cheap
    }

    /// List all view names
    pub fn list_views(&self) -> Result<Vec<String>> {
        if !self.is_open() {
            return Err(Error::EngineNotOpen);
        }

        let views = self.views.read().unwrap();
        Ok(views.values().map(|v| v.original_name.clone()).collect())
    }
}
