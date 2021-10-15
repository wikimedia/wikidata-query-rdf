<?php

$wgShowExceptionDetails = true;



$wgMWOAuthSecureTokenTransfer = false;

$wgGroupPermissions['*']['mwoauthproposeconsumer'] = true;
$wgGroupPermissions['*']['mwoauthupdateownconsumer'] = true;
$wgGroupPermissions['*']['mwoauthmanageconsumer'] = true;
$wgGroupPermissions['*']['mwoauthsuppress'] = true;
$wgGroupPermissions['*']['mwoauthviewsuppressed'] = true;
$wgGroupPermissions['*']['mwoauthviewprivate'] = true;
$wgGroupPermissions['*']['mwoauthmanagemygrants'] = true;

$wgHooks['EmailConfirmed'][] = function ( $user, &$confirmed ) {
    $confirmed = !$user->isAnon();
    return false;
};
