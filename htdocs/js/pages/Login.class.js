Class.subclass( Page.Base, "Page.Login", {	
	
	onInit: function() {
		// called once at page load
		// var html = 'Now is the time (LOGIN)';
		// this.div.html( html );
	},
	
	onActivate: function(args) {
		// Consume token fragments before normal authenticated navigation.
		if (args && args.verify) {
			var action = window.owlcronAccountAction;
			delete window.owlcronAccountAction;
			this.showEmailVerification(action);
			return true;
		}
		if (app.user) {
			// user already logged in
			setTimeout( function() { Nav.go(app.navAfterLogin || config.DefaultPage) }, 1 );
			return true;
		}
		else if (args && (args.create || args.recover || args.u || args.h)) {
			this.showAccountHelp();
			return true;
		}

		app.setWindowTitle('Login');
		app.showTabBar(false);
		
		this.div.css({ 'padding-top':'75px', 'padding-bottom':'75px' });
		var html = '';
		
		html += '<div class="inline_dialog_container">';
			html += '<div class="dialog_title shade-light">User Login</div>';
			html += '<div class="dialog_content">';
				html += '<center><table style="margin:0px;">';
					html += '<tr>';
						html += '<td align="right" class="table_label">Username:</td>';
						html += '<td align="left" class="table_value"><div><input type="text" name="username" id="fe_login_username" size="30" spellcheck="false" value="'+(app.getPref('username') || '')+'"/></div></td>';
					html += '</tr>';
					html += '<tr><td colspan="2"><div class="table_spacer"></div></td></tr>';
					html += '<tr>';
						html += '<td align="right" class="table_label">Password:</td>';
						html += '<td align="left" class="table_value"><div><input type="' + app.get_password_type() + '" name="password" id="fe_login_password" size="30" spellcheck="false" value=""/>' + app.get_password_toggle_html() + '</div></td>';
					html += '</tr>';
					html += '<tr><td colspan="2"><div class="table_spacer"></div></td></tr>';
				html += '</table></center>';
			html += '</div>';
			
			html += '<div class="dialog_buttons"><center><table><tr>';
				html += '<td><div class="button" style="width:120px;" onMouseUp="$P().doLogin()"><i class="fa fa-sign-in">&nbsp;&nbsp;</i>Login</div></td>';
			html += '</tr></table></center></div>';
		html += '</div>';
		
		html += '<div class="caption" style="text-align:center; margin-top:15px;">Need an account or a password reset? <a href="#Login?recover=1">Account access help</a></div>';
		this.div.html( html );
		
		setTimeout( function() {
			$( app.getPref('username') ? '#fe_login_password' : '#fe_login_username' ).focus();
			
			 $('#fe_login_username, #fe_login_password').keypress( function(event) {
				if (event.keyCode == '13') { // enter key
					event.preventDefault();
					$P().doLogin();
				}
			} ); 
			
		}, 1 );
		
		return true;
	},
	
	 doLogin: function() {
		// attempt to log user in
		var username = $('#fe_login_username').val();
		var password = $('#fe_login_password').val();
		
		if (username && password) {
			app.showProgress(1.0, "Logging in...");
			
			app.api.post( 'user/login', {
				username: username,
				password: password
			}, 
			function(resp, tx) {
				Debug.trace("User Login: " + username);
				
				app.hideProgress();
				app.doUserLogin( resp );
				
				Nav.go( app.navAfterLogin || config.DefaultPage );
			} ); // post
		}
	}, 
	

	showEmailVerification: function(action) {
		app.setWindowTitle('Verify Email');
		app.showTabBar(false);
		this.pendingEmailAction = null;
		var valid = config.email_verification && action && action.purpose === 'verify' &&
			typeof action.userID === 'string' && /^[a-zA-Z0-9_.-]{1,128}$/.test(action.userID) &&
			typeof action.token === 'string' && /^[a-f0-9]{64}$/.test(action.token);
		if (valid) this.pendingEmailAction = action;
		this.div.html('<div class="inline_dialog_container"><div class="dialog_title shade-light">Verify Email</div>' +
			'<div class="dialog_content"><p>' + (valid ?
			'Confirm that you want to use the email address from this verification request.' :
			'This verification link is unavailable. Sign in and request a new link from My Account, or contact your administrator.') + '</p></div>' +
			'<div class="dialog_buttons">' + (valid ? '<div class="button" id="confirm_email_verification" onMouseUp="$P().confirmEmailVerification()">Verify Email</div>' : '') +
			'<a class="button" href="#Login">Back to Login</a></div></div>');
	},

	confirmEmailVerification: function() {
		var action = this.pendingEmailAction;
		if (!action) return;
		this.pendingEmailAction = null;
		var self = this;
		app.api.postAccountAction('user/verify_email', {user_id:action.userID, token:action.token}, function() {
			self.div.html('<div class="inline_dialog_container"><div class="dialog_title shade-light">Email Verified</div>' +
				'<div class="dialog_content"><p>Your email address has been verified. Return to your account to continue.</p></div>' +
				'<div class="dialog_buttons"><a class="button" href="#MyAccount">Continue</a></div></div>');
		}, function() {
			self.showEmailVerification(null);
		});
	},

	showAccountHelp: function() {
		app.setWindowTitle('Account Access');
		app.showTabBar(false);
		this.div.css({ 'padding-top':'75px', 'padding-bottom':'75px' });
		this.div.html('<div class="inline_dialog_container">' +
			'<div class="dialog_title shade-light">Account Access</div>' +
			'<div class="dialog_content"><p>Accounts are created by your OwlCron administrator. Self-service registration and password recovery by email are not available.</p>' +
			'<p>If you cannot sign in, contact your administrator to create an account or reset your password. Existing Cronicle password reset links cannot be used here.</p>' +
			'<p>If you can sign in, change your password from My Account using your current password.</p></div>' +
			'<div class="dialog_buttons"><a class="button" href="#Login">Back to Login</a></div></div>');
	},
	
	onDeactivate: function() {
		// called when page is deactivated
		this.pendingEmailAction = null;
		this.div.html( '' );
		return true;
	}
	
} );
