// Keyboard access for the legacy mouse-driven controls and modal framework.
(function() {
	'use strict';
	var actions = '[onmouseup], [onclick], .tab[id^="tab_"]';
	var focusable = 'a[href], button, input, select, textarea, [tabindex]';
	var returnFocus = null;

	function enhance(root) {
		var nodes = Array.prototype.slice.call(root.querySelectorAll(actions));
		if (root.matches && root.matches(actions)) nodes.unshift(root);
		nodes.forEach(function(node) {
			if (/^(INPUT|SELECT|TEXTAREA|BUTTON)$/.test(node.tagName)) return;
			if (node.tagName === 'A' && node.hasAttribute('href')) return;
			if (!node.hasAttribute('role')) node.setAttribute('role', 'button');
			if (!node.hasAttribute('tabindex')) node.setAttribute('tabindex', '0');
			if (!node.textContent.trim() && node.title && !node.hasAttribute('aria-label')) {
				node.setAttribute('aria-label', node.title);
			}
		});
	}

	function enhanceScrollRegions() {
		Array.prototype.forEach.call(document.querySelectorAll('table.data_table, .pagination'), function(node) {
			if (node.scrollWidth > node.clientWidth + 1 && (node.dataset.keyboardScroll || (!node.hasAttribute('tabindex') && !node.hasAttribute('role')))) {
				node.setAttribute('tabindex', '0');
				if (node.tagName !== 'TABLE' && !node.hasAttribute('role')) {
					node.setAttribute('role', 'region');
					node.dataset.keyboardScrollRole = 'true';
				}
				if (!node.hasAttribute('aria-label')) {
					node.setAttribute('aria-label', node.tagName === 'TABLE' ? 'Data table, scroll horizontally' : 'Pagination, scroll horizontally');
					node.dataset.keyboardScrollLabel = 'true';
				}
				node.dataset.keyboardScroll = 'true';
			}
			else if (node.dataset.keyboardScroll) {
				node.removeAttribute('tabindex');
				if (node.dataset.keyboardScrollRole) node.removeAttribute('role');
				delete node.dataset.keyboardScrollRole;
				if (node.dataset.keyboardScrollLabel) node.removeAttribute('aria-label');
				delete node.dataset.keyboardScrollLabel;
				delete node.dataset.keyboardScroll;
			}
		});
	}

	function visible(node) {
		return !node.disabled && node.getAttribute('aria-disabled') !== 'true' &&
			node.tabIndex >= 0 && node.getClientRects().length > 0;
	}

	function controls(dialog) {
		return Array.prototype.filter.call(dialog.querySelectorAll(focusable), visible);
	}

	var show = Dialog.show;
	Dialog.show = function() {
		if (!this.active) returnFocus = document.activeElement;
		var result = show.apply(this, arguments);
		var dialog = document.getElementById('dialog_container');
		if (dialog) {
			dialog.setAttribute('role', 'dialog');
			dialog.setAttribute('aria-modal', 'true');
			dialog.setAttribute('tabindex', '-1');
			var title = dialog.querySelector('.dialog_title, .tab .content');
			if (title) {
				title.id = 'owlcron_dialog_title';
				dialog.setAttribute('aria-labelledby', title.id);
			}
			enhance(dialog);
			(controls(dialog)[0] || dialog).focus();
		}
		return result;
	};
	var hide = Dialog.hide;
	Dialog.hide = function() {
		var wasActive = this.active;
		var closing = document.getElementById('dialog_container');
		if (wasActive && closing) closing.inert = true;
		var result = hide.apply(this, arguments);
		if (wasActive && returnFocus && returnFocus.isConnected) returnFocus.focus();
		if (wasActive) returnFocus = null;
		return result;
	};

	document.addEventListener('keydown', function(event) {
		var dialog = Dialog.active && document.getElementById('dialog_container');
		if (dialog && event.key === 'Escape') {
			event.preventDefault();
			event.stopImmediatePropagation();
			if (Dialog.active === 'confirmation') app.confirm_click(false);
			else if (!Dialog.clickBlock) Dialog.hide();
			return;
		}
		if (dialog && event.key === 'Tab') {
			var list = controls(dialog);
			var first = list[0] || dialog;
			var last = list[list.length - 1] || dialog;
			if (!dialog.contains(document.activeElement) || !list.length ||
				(event.shiftKey && (document.activeElement === first || document.activeElement === dialog)) ||
				(!event.shiftKey && document.activeElement === last)) {
				event.preventDefault();
				(event.shiftKey ? last : first).focus();
			}
		}
		if (event.key !== 'Enter' && event.key !== ' ') return;
		var target = event.target;
		// Keep native button/link defaults, without the framework treating Enter as Confirm.
		if (dialog && event.key === 'Enter' && target.closest && target.closest('button, a[href]')) event.stopPropagation();
		if (target.closest && target.closest('input, select, textarea, [contenteditable="true"]')) return;
		var action = target.closest && target.closest(actions);
		if (!action || /^(INPUT|SELECT|TEXTAREA|BUTTON)$/.test(action.tagName)) return;
		if (action.getAttribute('aria-disabled') === 'true') return;
		// A link with only onclick already has native Enter activation.
		if (action.tagName === 'A' && action.hasAttribute('href') && !action.hasAttribute('onmouseup') && event.key === 'Enter') return;
		event.preventDefault();
		event.stopImmediatePropagation();
		if (event.repeat) return;
		action.dispatchEvent(new MouseEvent(action.hasAttribute('onmouseup') ? 'mouseup' : 'click', {bubbles:true, cancelable:true, view:window, shiftKey:event.shiftKey, ctrlKey:event.ctrlKey, altKey:event.altKey, metaKey:event.metaKey}));
	}, true);

	function init() {
		enhance(document);
		enhanceScrollRegions();
		window.addEventListener('resize', enhanceScrollRegions);
		new MutationObserver(function(records) {
			records.forEach(function(record) {
				Array.prototype.forEach.call(record.addedNodes, function(node) {
					if (node.nodeType === 1) enhance(node);
				});
			});
			enhanceScrollRegions();
		}).observe(document.body, {childList:true, subtree:true});
	}
	if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, {once:true});
	else init();
})();
