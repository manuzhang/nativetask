




<!DOCTYPE html>
<html class="   ">
  <head prefix="og: http://ogp.me/ns# fb: http://ogp.me/ns/fb# object: http://ogp.me/ns/object# article: http://ogp.me/ns/article# profile: http://ogp.me/ns/profile#">
    <meta charset='utf-8'>
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    
    
    <title>nativetask/README.md at master · intel-hadoop/nativetask</title>
    <link rel="search" type="application/opensearchdescription+xml" href="/opensearch.xml" title="GitHub" />
    <link rel="fluid-icon" href="https://github.com/fluidicon.png" title="GitHub" />
    <link rel="apple-touch-icon" sizes="57x57" href="/apple-touch-icon-114.png" />
    <link rel="apple-touch-icon" sizes="114x114" href="/apple-touch-icon-114.png" />
    <link rel="apple-touch-icon" sizes="72x72" href="/apple-touch-icon-144.png" />
    <link rel="apple-touch-icon" sizes="144x144" href="/apple-touch-icon-144.png" />
    <meta property="fb:app_id" content="1401488693436528"/>

      <meta content="@github" name="twitter:site" /><meta content="summary" name="twitter:card" /><meta content="intel-hadoop/nativetask" name="twitter:title" /><meta content="nativetask - native task" name="twitter:description" /><meta content="https://avatars0.githubusercontent.com/u/1839373?s=400" name="twitter:image:src" />
<meta content="GitHub" property="og:site_name" /><meta content="object" property="og:type" /><meta content="https://avatars0.githubusercontent.com/u/1839373?s=400" property="og:image" /><meta content="intel-hadoop/nativetask" property="og:title" /><meta content="https://github.com/intel-hadoop/nativetask" property="og:url" /><meta content="nativetask - native task" property="og:description" />

    <link rel="assets" href="https://assets-cdn.github.com/">
    <link rel="conduit-xhr" href="https://ghconduit.com:25035/">
    <link rel="xhr-socket" href="/_sockets" />

    <meta name="msapplication-TileImage" content="/windows-tile.png" />
    <meta name="msapplication-TileColor" content="#ffffff" />
    <meta name="selected-link" value="repo_source" data-pjax-transient />
    <meta content="collector.githubapp.com" name="octolytics-host" /><meta content="collector-cdn.github.com" name="octolytics-script-host" /><meta content="github" name="octolytics-app-id" /><meta content="86868949:522C:1C9AC23:537842FC" name="octolytics-dimension-request_id" /><meta content="1191767" name="octolytics-actor-id" /><meta content="manuzhang" name="octolytics-actor-login" /><meta content="14d2ccc9e231b3dd267698ebecb4938696ab6e7059c433da2bc505dd0f96e844" name="octolytics-actor-hash" />
    

    
    
    <link rel="icon" type="image/x-icon" href="https://assets-cdn.github.com/favicon.ico" />

    <meta content="authenticity_token" name="csrf-param" />
<meta content="KLMMAnDX6K78oT/AhgOcu1gHfpvA+dtWj+2yiidrT11owaUL2W1eebZlIpwqBQNCn7JvVOiA0eyBY7/+R8OTMw==" name="csrf-token" />

    <link href="https://assets-cdn.github.com/assets/github-da7475c114a3c2eab7b91a0584202a89fd188935.css" media="all" rel="stylesheet" type="text/css" />
    <link href="https://assets-cdn.github.com/assets/github2-c113bb3e86e5e802a9d3d9ae473dcfe049699e63.css" media="all" rel="stylesheet" type="text/css" />
    


    <meta http-equiv="x-pjax-version" content="18d419c56724646a83499312832c5ab3">

      
  <meta name="description" content="nativetask - native task" />

  <meta content="1839373" name="octolytics-dimension-user_id" /><meta content="intel-hadoop" name="octolytics-dimension-user_login" /><meta content="11889344" name="octolytics-dimension-repository_id" /><meta content="intel-hadoop/nativetask" name="octolytics-dimension-repository_nwo" /><meta content="true" name="octolytics-dimension-repository_public" /><meta content="true" name="octolytics-dimension-repository_is_fork" /><meta content="10469715" name="octolytics-dimension-repository_parent_id" /><meta content="clockfly/nativetask" name="octolytics-dimension-repository_parent_nwo" /><meta content="2836057" name="octolytics-dimension-repository_network_root_id" /><meta content="decster/nativetask" name="octolytics-dimension-repository_network_root_nwo" />
  <link href="https://github.com/intel-hadoop/nativetask/commits/master.atom" rel="alternate" title="Recent Commits to nativetask:master" type="application/atom+xml" />

  </head>


  <body class="logged_in  env-production windows vis-public fork page-blob">
    <a href="#start-of-content" tabindex="1" class="accessibility-aid js-skip-to-content">Skip to content</a>
    <div class="wrapper">
      
      
      
      


      <div class="header header-logged-in true">
  <div class="container clearfix">

    <a class="header-logo-invertocat" href="https://github.com/">
  <span class="mega-octicon octicon-mark-github"></span>
</a>

    
    <a href="/notifications" aria-label="You have unread notifications" class="notification-indicator tooltipped tooltipped-s" data-hotkey="g n">
        <span class="mail-status unread"></span>
</a>

      <div class="command-bar js-command-bar  in-repository">
          <form accept-charset="UTF-8" action="/search" class="command-bar-form" id="top_search_form" method="get">

<div class="commandbar">
  <span class="message"></span>
  <input type="text" data-hotkey="s, /" name="q" id="js-command-bar-field" placeholder="Search or type a command" tabindex="1" autocapitalize="off"
    
    data-username="manuzhang"
      data-repo="intel-hadoop/nativetask"
      data-branch="master"
      data-sha="011c8d99a84f78cd5375fa60d6e55b3cd4fc77c4"
  >
  <div class="display hidden"></div>
</div>

    <input type="hidden" name="nwo" value="intel-hadoop/nativetask" />

    <div class="select-menu js-menu-container js-select-menu search-context-select-menu">
      <span class="minibutton select-menu-button js-menu-target" role="button" aria-haspopup="true">
        <span class="js-select-button">This repository</span>
      </span>

      <div class="select-menu-modal-holder js-menu-content js-navigation-container" aria-hidden="true">
        <div class="select-menu-modal">

          <div class="select-menu-item js-navigation-item js-this-repository-navigation-item selected">
            <span class="select-menu-item-icon octicon octicon-check"></span>
            <input type="radio" class="js-search-this-repository" name="search_target" value="repository" checked="checked" />
            <div class="select-menu-item-text js-select-button-text">This repository</div>
          </div> <!-- /.select-menu-item -->

          <div class="select-menu-item js-navigation-item js-all-repositories-navigation-item">
            <span class="select-menu-item-icon octicon octicon-check"></span>
            <input type="radio" name="search_target" value="global" />
            <div class="select-menu-item-text js-select-button-text">All repositories</div>
          </div> <!-- /.select-menu-item -->

        </div>
      </div>
    </div>

  <span class="help tooltipped tooltipped-s" aria-label="Show command bar help">
    <span class="octicon octicon-question"></span>
  </span>


  <input type="hidden" name="ref" value="cmdform">

</form>
        <ul class="top-nav">
          <li class="explore"><a href="/explore">Explore</a></li>
            <li><a href="https://gist.github.com">Gist</a></li>
            <li><a href="/blog">Blog</a></li>
          <li><a href="https://help.github.com">Help</a></li>
        </ul>
      </div>

    


  <ul id="user-links">
    <li>
      <a href="/manuzhang" class="name">
        <img alt="ManuZhang" class=" js-avatar" data-user="1191767" height="20" src="https://avatars3.githubusercontent.com/u/1191767?s=140" width="20" /> manuzhang
      </a>
    </li>

    <li class="new-menu dropdown-toggle js-menu-container">
      <a href="#" class="js-menu-target tooltipped tooltipped-s" aria-label="Create new...">
        <span class="octicon octicon-plus"></span>
        <span class="dropdown-arrow"></span>
      </a>

      <div class="new-menu-content js-menu-content">
      </div>
    </li>

    <li>
      <a href="/settings/profile" id="account_settings"
        class="tooltipped tooltipped-s"
        aria-label="Account settings ">
        <span class="octicon octicon-tools"></span>
      </a>
    </li>
    <li>
      <form class="logout-form" action="/logout" method="post">
        <button class="sign-out-button tooltipped tooltipped-s" aria-label="Sign out">
          <span class="octicon octicon-log-out"></span>
        </button>
      </form>
    </li>

  </ul>

<div class="js-new-dropdown-contents hidden">
  

<ul class="dropdown-menu">
  <li>
    <a href="/new"><span class="octicon octicon-repo-create"></span> New repository</a>
  </li>
  <li>
    <a href="/organizations/new"><span class="octicon octicon-organization"></span> New organization</a>
  </li>


</ul>

</div>


    
  </div>
</div>

      

        



      <div id="start-of-content" class="accessibility-aid"></div>
          <div class="site" itemscope itemtype="http://schema.org/WebPage">
    <div id="js-flash-container">
      
    </div>
    <div class="pagehead repohead instapaper_ignore readability-menu">
      <div class="container">
        

<ul class="pagehead-actions">

    <li class="subscription">
      <form accept-charset="UTF-8" action="/notifications/subscribe" class="js-social-container" data-autosubmit="true" data-remote="true" method="post"><div style="margin:0;padding:0;display:inline"><input name="authenticity_token" type="hidden" value="xxgSrEN5enyyZRG+8kzspR8t4I0Yr+B/fbbjcZgO3b6uIH9YFRZ6TWUDT/OParoH9AXuVoktLnQP97uLxRQd5g==" /></div>  <input id="repository_id" name="repository_id" type="hidden" value="11889344" />

    <div class="select-menu js-menu-container js-select-menu">
      <a class="social-count js-social-count" href="/intel-hadoop/nativetask/watchers">
        17
      </a>
      <span class="minibutton select-menu-button with-count js-menu-target" role="button" tabindex="0" aria-haspopup="true">
        <span class="js-select-button">
          <span class="octicon octicon-eye-unwatch"></span>
          Unwatch
        </span>
      </span>

      <div class="select-menu-modal-holder">
        <div class="select-menu-modal subscription-menu-modal js-menu-content" aria-hidden="true">
          <div class="select-menu-header">
            <span class="select-menu-title">Notification status</span>
            <span class="octicon octicon-remove-close js-menu-close"></span>
          </div> <!-- /.select-menu-header -->

          <div class="select-menu-list js-navigation-container" role="menu">

            <div class="select-menu-item js-navigation-item " role="menuitem" tabindex="0">
              <span class="select-menu-item-icon octicon octicon-check"></span>
              <div class="select-menu-item-text">
                <input id="do_included" name="do" type="radio" value="included" />
                <h4>Not watching</h4>
                <span class="description">You only receive notifications for conversations in which you participate or are @mentioned.</span>
                <span class="js-select-button-text hidden-select-button-text">
                  <span class="octicon octicon-eye-watch"></span>
                  Watch
                </span>
              </div>
            </div> <!-- /.select-menu-item -->

            <div class="select-menu-item js-navigation-item selected" role="menuitem" tabindex="0">
              <span class="select-menu-item-icon octicon octicon octicon-check"></span>
              <div class="select-menu-item-text">
                <input checked="checked" id="do_subscribed" name="do" type="radio" value="subscribed" />
                <h4>Watching</h4>
                <span class="description">You receive notifications for all conversations in this repository.</span>
                <span class="js-select-button-text hidden-select-button-text">
                  <span class="octicon octicon-eye-unwatch"></span>
                  Unwatch
                </span>
              </div>
            </div> <!-- /.select-menu-item -->

            <div class="select-menu-item js-navigation-item " role="menuitem" tabindex="0">
              <span class="select-menu-item-icon octicon octicon-check"></span>
              <div class="select-menu-item-text">
                <input id="do_ignore" name="do" type="radio" value="ignore" />
                <h4>Ignoring</h4>
                <span class="description">You do not receive any notifications for conversations in this repository.</span>
                <span class="js-select-button-text hidden-select-button-text">
                  <span class="octicon octicon-mute"></span>
                  Stop ignoring
                </span>
              </div>
            </div> <!-- /.select-menu-item -->

          </div> <!-- /.select-menu-list -->

        </div> <!-- /.select-menu-modal -->
      </div> <!-- /.select-menu-modal-holder -->
    </div> <!-- /.select-menu -->

</form>
    </li>

  <li>
  

  <div class="js-toggler-container js-social-container starring-container ">

    <form accept-charset="UTF-8" action="/intel-hadoop/nativetask/unstar" class="js-toggler-form starred" data-remote="true" method="post"><div style="margin:0;padding:0;display:inline"><input name="authenticity_token" type="hidden" value="9As5+I4LhkNwwVVIVQo6dBHk9jlw13pnNUtpGyM+zEE0uLLgGyVRJY5D+1jgXLipWTBivOcttJ6O5wUs5qCl3w==" /></div>
      <button
        class="minibutton with-count js-toggler-target star-button"
        aria-label="Unstar this repository" title="Unstar intel-hadoop/nativetask">
        <span class="octicon octicon-star-delete"></span><span class="text">Unstar</span>
      </button>
        <a class="social-count js-social-count" href="/intel-hadoop/nativetask/stargazers">
          2
        </a>
</form>
    <form accept-charset="UTF-8" action="/intel-hadoop/nativetask/star" class="js-toggler-form unstarred" data-remote="true" method="post"><div style="margin:0;padding:0;display:inline"><input name="authenticity_token" type="hidden" value="L8YUKnETK/nPBBdcZgJfZsiigpPbTELixlBtozGHIUm+OTD1hMTq5lqRl+yabDnp+2KIYx+GlrvAUB6Th91unw==" /></div>
      <button
        class="minibutton with-count js-toggler-target star-button"
        aria-label="Star this repository" title="Star intel-hadoop/nativetask">
        <span class="octicon octicon-star"></span><span class="text">Star</span>
      </button>
        <a class="social-count js-social-count" href="/intel-hadoop/nativetask/stargazers">
          2
        </a>
</form>  </div>

  </li>


        <li>
          <a href="/intel-hadoop/nativetask/fork" class="minibutton with-count js-toggler-target fork-button lighter tooltipped-n" title="Fork your own copy of intel-hadoop/nativetask to your account" aria-label="Fork your own copy of intel-hadoop/nativetask to your account" rel="facebox nofollow">
            <span class="octicon octicon-git-branch-create"></span><span class="text">Fork</span>
          </a>
          <a href="/intel-hadoop/nativetask/network" class="social-count">12</a>
        </li>


</ul>

        <h1 itemscope itemtype="http://data-vocabulary.org/Breadcrumb" class="entry-title public">
          <span class="repo-label"><span>public</span></span>
          <span class="mega-octicon octicon-repo-forked"></span>
          <span class="author"><a href="/intel-hadoop" class="url fn" itemprop="url" rel="author"><span itemprop="title">intel-hadoop</span></a></span><!--
       --><span class="path-divider">/</span><!--
       --><strong><a href="/intel-hadoop/nativetask" class="js-current-repository js-repo-home-link">nativetask</a></strong>

          <span class="page-context-loader">
            <img alt="Octocat-spinner-32" height="16" src="https://assets-cdn.github.com/images/spinners/octocat-spinner-32.gif" width="16" />
          </span>

            <span class="fork-flag">
              <span class="text">forked from <a href="/clockfly/nativetask">clockfly/nativetask</a></span>
            </span>
        </h1>
      </div><!-- /.container -->
    </div><!-- /.repohead -->

    <div class="container">
      <div class="repository-with-sidebar repo-container new-discussion-timeline js-new-discussion-timeline  ">
        <div class="repository-sidebar clearfix">
            

<div class="sunken-menu vertical-right repo-nav js-repo-nav js-repository-container-pjax js-octicon-loaders">
  <div class="sunken-menu-contents">
    <ul class="sunken-menu-group">
      <li class="tooltipped tooltipped-w" aria-label="Code">
        <a href="/intel-hadoop/nativetask" aria-label="Code" class="selected js-selected-navigation-item sunken-menu-item" data-hotkey="g c" data-pjax="true" data-selected-links="repo_source repo_downloads repo_commits repo_releases repo_tags repo_branches /intel-hadoop/nativetask">
          <span class="octicon octicon-code"></span> <span class="full-word">Code</span>
          <img alt="Octocat-spinner-32" class="mini-loader" height="16" src="https://assets-cdn.github.com/images/spinners/octocat-spinner-32.gif" width="16" />
</a>      </li>


      <li class="tooltipped tooltipped-w" aria-label="Pull Requests">
        <a href="/intel-hadoop/nativetask/pulls" aria-label="Pull Requests" class="js-selected-navigation-item sunken-menu-item js-disable-pjax" data-hotkey="g p" data-selected-links="repo_pulls /intel-hadoop/nativetask/pulls">
            <span class="octicon octicon-git-pull-request"></span> <span class="full-word">Pull Requests</span>
            <span class='counter'>0</span>
            <img alt="Octocat-spinner-32" class="mini-loader" height="16" src="https://assets-cdn.github.com/images/spinners/octocat-spinner-32.gif" width="16" />
</a>      </li>


        <li class="tooltipped tooltipped-w" aria-label="Wiki">
          <a href="/intel-hadoop/nativetask/wiki" aria-label="Wiki" class="js-selected-navigation-item sunken-menu-item js-disable-pjax" data-hotkey="g w" data-selected-links="repo_wiki /intel-hadoop/nativetask/wiki">
            <span class="octicon octicon-book"></span> <span class="full-word">Wiki</span>
            <img alt="Octocat-spinner-32" class="mini-loader" height="16" src="https://assets-cdn.github.com/images/spinners/octocat-spinner-32.gif" width="16" />
</a>        </li>
    </ul>
    <div class="sunken-menu-separator"></div>
    <ul class="sunken-menu-group">

      <li class="tooltipped tooltipped-w" aria-label="Pulse">
        <a href="/intel-hadoop/nativetask/pulse" aria-label="Pulse" class="js-selected-navigation-item sunken-menu-item" data-pjax="true" data-selected-links="pulse /intel-hadoop/nativetask/pulse">
          <span class="octicon octicon-pulse"></span> <span class="full-word">Pulse</span>
          <img alt="Octocat-spinner-32" class="mini-loader" height="16" src="https://assets-cdn.github.com/images/spinners/octocat-spinner-32.gif" width="16" />
</a>      </li>

      <li class="tooltipped tooltipped-w" aria-label="Graphs">
        <a href="/intel-hadoop/nativetask/graphs" aria-label="Graphs" class="js-selected-navigation-item sunken-menu-item" data-pjax="true" data-selected-links="repo_graphs repo_contributors /intel-hadoop/nativetask/graphs">
          <span class="octicon octicon-graph"></span> <span class="full-word">Graphs</span>
          <img alt="Octocat-spinner-32" class="mini-loader" height="16" src="https://assets-cdn.github.com/images/spinners/octocat-spinner-32.gif" width="16" />
</a>      </li>

      <li class="tooltipped tooltipped-w" aria-label="Network">
        <a href="/intel-hadoop/nativetask/network" aria-label="Network" class="js-selected-navigation-item sunken-menu-item js-disable-pjax" data-selected-links="repo_network /intel-hadoop/nativetask/network">
          <span class="octicon octicon-git-branch"></span> <span class="full-word">Network</span>
          <img alt="Octocat-spinner-32" class="mini-loader" height="16" src="https://assets-cdn.github.com/images/spinners/octocat-spinner-32.gif" width="16" />
</a>      </li>
    </ul>


  </div>
</div>

              <div class="only-with-full-nav">
                

  

<div class="clone-url open"
  data-protocol-type="http"
  data-url="/users/set_protocol?protocol_selector=http&amp;protocol_type=push">
  <h3><strong>HTTPS</strong> clone URL</h3>
  <div class="clone-url-box">
    <input type="text" class="clone js-url-field"
           value="https://github.com/intel-hadoop/nativetask.git" readonly="readonly">
    <span class="url-box-clippy">
    <button aria-label="copy to clipboard" class="js-zeroclipboard minibutton zeroclipboard-button" data-clipboard-text="https://github.com/intel-hadoop/nativetask.git" data-copied-hint="copied!" type="button"><span class="octicon octicon-clippy"></span></button>
    </span>
  </div>
</div>

  

<div class="clone-url "
  data-protocol-type="ssh"
  data-url="/users/set_protocol?protocol_selector=ssh&amp;protocol_type=push">
  <h3><strong>SSH</strong> clone URL</h3>
  <div class="clone-url-box">
    <input type="text" class="clone js-url-field"
           value="git@github.com:intel-hadoop/nativetask.git" readonly="readonly">
    <span class="url-box-clippy">
    <button aria-label="copy to clipboard" class="js-zeroclipboard minibutton zeroclipboard-button" data-clipboard-text="git@github.com:intel-hadoop/nativetask.git" data-copied-hint="copied!" type="button"><span class="octicon octicon-clippy"></span></button>
    </span>
  </div>
</div>

  

<div class="clone-url "
  data-protocol-type="subversion"
  data-url="/users/set_protocol?protocol_selector=subversion&amp;protocol_type=push">
  <h3><strong>Subversion</strong> checkout URL</h3>
  <div class="clone-url-box">
    <input type="text" class="clone js-url-field"
           value="https://github.com/intel-hadoop/nativetask" readonly="readonly">
    <span class="url-box-clippy">
    <button aria-label="copy to clipboard" class="js-zeroclipboard minibutton zeroclipboard-button" data-clipboard-text="https://github.com/intel-hadoop/nativetask" data-copied-hint="copied!" type="button"><span class="octicon octicon-clippy"></span></button>
    </span>
  </div>
</div>


<p class="clone-options">You can clone with
      <a href="#" class="js-clone-selector" data-protocol="http">HTTPS</a>,
      <a href="#" class="js-clone-selector" data-protocol="ssh">SSH</a>,
      or <a href="#" class="js-clone-selector" data-protocol="subversion">Subversion</a>.
  <span class="help tooltipped tooltipped-n" aria-label="Get help on which URL is right for you.">
    <a href="https://help.github.com/articles/which-remote-url-should-i-use">
    <span class="octicon octicon-question"></span>
    </a>
  </span>
</p>


  <a href="github-windows://openRepo/https://github.com/intel-hadoop/nativetask" class="minibutton sidebar-button" title="Save intel-hadoop/nativetask to your computer and use it in GitHub Desktop." aria-label="Save intel-hadoop/nativetask to your computer and use it in GitHub Desktop.">
    <span class="octicon octicon-device-desktop"></span>
    Clone in Desktop
  </a>

                <a href="/intel-hadoop/nativetask/archive/master.zip"
                   class="minibutton sidebar-button"
                   aria-label="Download intel-hadoop/nativetask as a zip file"
                   title="Download intel-hadoop/nativetask as a zip file"
                   rel="nofollow">
                  <span class="octicon octicon-cloud-download"></span>
                  Download ZIP
                </a>
              </div>
        </div><!-- /.repository-sidebar -->

        <div id="js-repo-pjax-container" class="repository-content context-loader-container" data-pjax-container>
          


<a href="/intel-hadoop/nativetask/blob/0589f653505cccac0be674b444a7ee9abc755f05/README.md" class="hidden js-permalink-shortcut" data-hotkey="y">Permalink</a>

<!-- blob contrib key: blob_contributors:v21:0697b6b194259fd34b2ddbbae61d28f7 -->

<p title="This is a placeholder element" class="js-history-link-replace hidden"></p>

<a href="/intel-hadoop/nativetask/find/master" data-pjax data-hotkey="t" class="js-show-file-finder" style="display:none">Show File Finder</a>

<div class="file-navigation">
  

<div class="select-menu js-menu-container js-select-menu" >
  <span class="minibutton select-menu-button js-menu-target" data-hotkey="w"
    data-master-branch="master"
    data-ref="master"
    role="button" aria-label="Switch branches or tags" tabindex="0" aria-haspopup="true">
    <span class="octicon octicon-git-branch"></span>
    <i>branch:</i>
    <span class="js-select-button">master</span>
  </span>

  <div class="select-menu-modal-holder js-menu-content js-navigation-container" data-pjax aria-hidden="true">

    <div class="select-menu-modal">
      <div class="select-menu-header">
        <span class="select-menu-title">Switch branches/tags</span>
        <span class="octicon octicon-remove-close js-menu-close"></span>
      </div> <!-- /.select-menu-header -->

      <div class="select-menu-filters">
        <div class="select-menu-text-filter">
          <input type="text" aria-label="Find or create a branch…" id="context-commitish-filter-field" class="js-filterable-field js-navigation-enable" placeholder="Find or create a branch…">
        </div>
        <div class="select-menu-tabs">
          <ul>
            <li class="select-menu-tab">
              <a href="#" data-tab-filter="branches" class="js-select-menu-tab">Branches</a>
            </li>
            <li class="select-menu-tab">
              <a href="#" data-tab-filter="tags" class="js-select-menu-tab">Tags</a>
            </li>
          </ul>
        </div><!-- /.select-menu-tabs -->
      </div><!-- /.select-menu-filters -->

      <div class="select-menu-list select-menu-tab-bucket js-select-menu-tab-bucket" data-tab-filter="branches">

        <div data-filterable-for="context-commitish-filter-field" data-filterable-type="substring">


            <div class="select-menu-item js-navigation-item ">
              <span class="select-menu-item-icon octicon octicon-check"></span>
              <a href="/intel-hadoop/nativetask/blob/gh-pages/README.md"
                 data-name="gh-pages"
                 data-skip-pjax="true"
                 rel="nofollow"
                 class="js-navigation-open select-menu-item-text js-select-button-text css-truncate-target"
                 title="gh-pages">gh-pages</a>
            </div> <!-- /.select-menu-item -->
            <div class="select-menu-item js-navigation-item selected">
              <span class="select-menu-item-icon octicon octicon-check"></span>
              <a href="/intel-hadoop/nativetask/blob/master/README.md"
                 data-name="master"
                 data-skip-pjax="true"
                 rel="nofollow"
                 class="js-navigation-open select-menu-item-text js-select-button-text css-truncate-target"
                 title="master">master</a>
            </div> <!-- /.select-menu-item -->
        </div>

          <form accept-charset="UTF-8" action="/intel-hadoop/nativetask/branches" class="js-create-branch select-menu-item select-menu-new-item-form js-navigation-item js-new-item-form" method="post"><div style="margin:0;padding:0;display:inline"><input name="authenticity_token" type="hidden" value="dexs6gQ8bAeLnfFlBoPnPQGFE4+gb7ccFGdubJlVJq8s5oNDRDqqupX5YPTCx0PwGaS8DOR0mbY0fg0ublYh/g==" /></div>
            <span class="octicon octicon-git-branch-create select-menu-item-icon"></span>
            <div class="select-menu-item-text">
              <h4>Create branch: <span class="js-new-item-name"></span></h4>
              <span class="description">from ‘master’</span>
            </div>
            <input type="hidden" name="name" id="name" class="js-new-item-value">
            <input type="hidden" name="branch" id="branch" value="master" />
            <input type="hidden" name="path" id="path" value="README.md" />
          </form> <!-- /.select-menu-item -->

      </div> <!-- /.select-menu-list -->

      <div class="select-menu-list select-menu-tab-bucket js-select-menu-tab-bucket" data-tab-filter="tags">
        <div data-filterable-for="context-commitish-filter-field" data-filterable-type="substring">


        </div>

        <div class="select-menu-no-results">Nothing to show</div>
      </div> <!-- /.select-menu-list -->

    </div> <!-- /.select-menu-modal -->
  </div> <!-- /.select-menu-modal-holder -->
</div> <!-- /.select-menu -->

  <div class="breadcrumb">
    <span class='repo-root js-repo-root'><span itemscope="" itemtype="http://data-vocabulary.org/Breadcrumb"><a href="/intel-hadoop/nativetask" data-branch="master" data-direction="back" data-pjax="true" itemscope="url"><span itemprop="title">nativetask</span></a></span></span><span class="separator"> / </span><strong class="final-path">README.md</strong> <button aria-label="copy to clipboard" class="js-zeroclipboard minibutton zeroclipboard-button" data-clipboard-text="README.md" data-copied-hint="copied!" type="button"><span class="octicon octicon-clippy"></span></button>
  </div>
</div>


  <div class="commit file-history-tease">
      <img alt="Sean Zhong" class="main-avatar js-avatar" data-user="2595532" height="24" src="https://avatars1.githubusercontent.com/u/2595532?s=140" width="24" />
      <span class="author"><a href="/clockfly" rel="contributor">clockfly</a></span>
      <time datetime="2014-05-18T13:12:12+08:00" is="relative-time" title-format="%Y-%m-%d %H:%M:%S %z" title="2014-05-18 13:12:12 +0800">May 18, 2014</time>
      <div class="commit-title">
          <a href="/intel-hadoop/nativetask/commit/0589f653505cccac0be674b444a7ee9abc755f05" class="message" data-pjax="true" title="Update README.md">Update README.md</a>
      </div>

    <div class="participation">
      <p class="quickstat"><a href="#blob_contributors_box" rel="facebox"><strong>2</strong>  contributors</a></p>
          <a class="avatar tooltipped tooltipped-s" aria-label="decster" href="/intel-hadoop/nativetask/commits/master/README.md?author=decster"><img alt="Binglin Chang" class=" js-avatar" data-user="193300" height="20" src="https://avatars2.githubusercontent.com/u/193300?s=140" width="20" /></a>
    <a class="avatar tooltipped tooltipped-s" aria-label="clockfly" href="/intel-hadoop/nativetask/commits/master/README.md?author=clockfly"><img alt="Sean Zhong" class=" js-avatar" data-user="2595532" height="20" src="https://avatars1.githubusercontent.com/u/2595532?s=140" width="20" /></a>


    </div>
    <div id="blob_contributors_box" style="display:none">
      <h2 class="facebox-header">Users who have contributed to this file</h2>
      <ul class="facebox-user-list">
          <li class="facebox-user-list-item">
            <img alt="Binglin Chang" class=" js-avatar" data-user="193300" height="24" src="https://avatars2.githubusercontent.com/u/193300?s=140" width="24" />
            <a href="/decster">decster</a>
          </li>
          <li class="facebox-user-list-item">
            <img alt="Sean Zhong" class=" js-avatar" data-user="2595532" height="24" src="https://avatars1.githubusercontent.com/u/2595532?s=140" width="24" />
            <a href="/clockfly">clockfly</a>
          </li>
      </ul>
    </div>
  </div>

<div class="file-box">
  <div class="file">
    <div class="meta clearfix">
      <div class="info file-name">
        <span class="icon"><b class="octicon octicon-file-text"></b></span>
        <span class="mode" title="File Mode">file</span>
        <span class="meta-divider"></span>
          <span>127 lines (87 sloc)</span>
          <span class="meta-divider"></span>
        <span>4.778 kb</span>
      </div>
      <div class="actions">
        <div class="button-group">
            <a class="minibutton tooltipped tooltipped-w"
               href="github-windows://openRepo/https://github.com/intel-hadoop/nativetask?branch=master&amp;filepath=README.md" aria-label="Open this file in GitHub for Windows">
                <span class="octicon octicon-device-desktop"></span> Open
            </a>
                <a class="minibutton js-update-url-with-hash"
                   href="/intel-hadoop/nativetask/edit/master/README.md"
                   data-method="post" rel="nofollow" data-hotkey="e">Edit</a>
          <a href="/intel-hadoop/nativetask/raw/master/README.md" class="button minibutton " id="raw-url">Raw</a>
            <a href="/intel-hadoop/nativetask/blame/master/README.md" class="button minibutton js-update-url-with-hash">Blame</a>
          <a href="/intel-hadoop/nativetask/commits/master/README.md" class="button minibutton " rel="nofollow">History</a>
        </div><!-- /.button-group -->

            <a class="minibutton danger empty-icon"
               href="/intel-hadoop/nativetask/delete/master/README.md"
               data-method="post" data-test-id="delete-blob-file" rel="nofollow">

          Delete
        </a>
      </div><!-- /.actions -->
    </div>
      
  <div id="readme" class="blob instapaper_body">
    <article class="markdown-body entry-content" itemprop="mainContentOfPage"><h1>
<a name="user-content-what-is-nativetask" class="anchor" href="#what-is-nativetask"><span class="octicon octicon-link"></span></a>What is NativeTask?</h1>

<p>NativeTask is a <strong>performance oriented</strong> native engine for Hadoop MapReduce.</p>

<p>NativeTask can be used transparently as a replacement of in-efficient Map Output Collector , or as a full native runtime which support native mapper and reducer written in C++. Please check paper for details <a href="http://prof.ict.ac.cn/bpoe2013/downloads/papers/S7201_5910.pdf"><em>NativeTask: A Hadoop Compatible Framework for High Performance</em></a>.</p>

<p>Some early discussions of NativeTask can be found at <a href="https://issues.apache.org/jira/browse/MAPREDUCE-2841">MAPREDUCE-2841</a>.</p>

<h1>
<a name="user-content-what-is-the-benefit" class="anchor" href="#what-is-the-benefit"><span class="octicon octicon-link"></span></a>What is the benefit?</h1>

<p><strong>1. Superior Performance</strong></p>

<p>For CPU intensive job like WordCount, we can provides <strong>2.6x</strong> performance boost transparently, or <strong>5x</strong> performance boost when running as full native runtime.
<a href="https://camo.githubusercontent.com/2ce6f21211b3e7d4b82127ea0ad9f7315cff3bb6/68747470733a2f2f6c68362e676f6f676c6575736572636f6e74656e742e636f6d2f2d436a316f6a6f526a4b786b2f553277324c46474c7a33492f41414141414141414331342f586e737473695568504b412f773935392d683535382d6e6f2f686962656e63682e504e47" target="_blank"><img src="https://camo.githubusercontent.com/2ce6f21211b3e7d4b82127ea0ad9f7315cff3bb6/68747470733a2f2f6c68362e676f6f676c6575736572636f6e74656e742e636f6d2f2d436a316f6a6f526a4b786b2f553277324c46474c7a33492f41414141414141414331342f586e737473695568504b412f773935392d683535382d6e6f2f686962656e63682e504e47" alt="native MapOutputCollector mode" data-canonical-src="https://lh6.googleusercontent.com/-Cj1ojoRjKxk/U2w2LFGLz3I/AAAAAAAAC14/XnstsiUhPKA/w959-h558-no/hibench.PNG" style="max-width:100%;"></a></p>

<p><strong>2. Compatibility and Transparency</strong></p>

<p>NativeTask can be transparently enabled in MRv1 and MRv2, requiring no code/binary change for existing MapReduce jobs. If certain required feature has not been supported yet, NativeTask will <strong>automatically fallback</strong> to default implementation.</p>

<p><strong>3. Feature Complete</strong></p>

<p>NativeTask is feature complete, it supports:</p>

<ul class="task-list">
<li>Most key types and all value types(subclass of Writable). For a comprehensive list of supported keys, please check the Wiki Page.</li>
<li>Platforms like HBase/Hive/Pig/Mahout. </li>
<li>Compression codec like Lz4/Snappy/Gzip.</li>
<li>Java/Native combiner.</li>
<li>Hardware checksumming CRC32C.</li>
<li>Non-sorting MapReduce paradigm when sorting is not required.</li>
</ul><p><strong>4. Full Extensibility</strong></p>

<p>Developers are allowed to extend NativeTask to support more key types, and to replace building blocks of NativeTask with a more efficient implementation dynamically without re-compilation of the source code.</p>

<h1>
<a name="user-content-how-to-use-nativetask" class="anchor" href="#how-to-use-nativetask"><span class="octicon octicon-link"></span></a>How to use NativeTask?</h1>

<p>NativeTask can works in two modes,</p>

<p><strong>1. Transparent Collector Mode.</strong> In this mode, NativeTask works as transparent replacement of current in-efficient Map Output Collector, with zero changes required from user side. </p>

<p><strong>2. Native Runtime Mode</strong> In this mode, NativeTask works as a dedicated native runtime to support native mapper and native reducer written in C++. </p>

<p>Here is the steps to enable NativeTask in transparent collector mode:</p>

<ol class="task-list">
<li>
<p>clone NativeTask repository</p>

<div class="highlight highlight-bash"><pre>git clone https://github.com/intel-hadoop/nativetask.git
</pre></div>
</li>
<li>
<p>Checkout the right source branch</p>

<p>To build NativeTask for hadoop1.2.1, </p>

<div class="highlight highlight-bash"><pre>git checkout hadoop-1.0
</pre></div>

<p>To build NativeTask for Hadoop2.2.0, </p>

<div class="highlight highlight-bash"><pre>git checkout master
</pre></div>
</li>
<li>
<p>patch Hadoop (${HADOOP_ROOTDIR} points to the root directory of Hadoop codebase)</p>

<div class="highlight highlight-bash"><pre><span class="nb">cd </span>nativetask
cp patch/hadoop-2.patch <span class="k">${</span><span class="nv">HADOOP_ROOTDIR</span><span class="k">}</span>/
<span class="nb">cd</span> <span class="k">${</span><span class="nv">HADOOP_ROOTDIR</span><span class="k">}</span>
patch -p0 &lt; hadoop-2.patch
</pre></div>
</li>
<li>
<p>build NativeTask with Hadoop</p>

<div class="highlight highlight-bash"><pre><span class="nb">cd </span>nativetask
cp -r . <span class="k">${</span><span class="nv">HADOOP_ROOTDIR</span><span class="k">}</span>/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask
<span class="nb">cd</span> <span class="k">${</span><span class="nv">HADOOP_ROOTDIR</span><span class="k">}</span>
mvn install -DskipTests -Pnative
</pre></div>
</li>
<li>
<p>install NativeTask </p>

<div class="highlight highlight-bash"><pre><span class="nb">cd</span> <span class="k">${</span><span class="nv">HADOOP_ROOTDIR</span><span class="k">}</span>/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/target
cp hadoop-mapreduce-client-nativetask-2.2.0.jar /usr/lib/hadoop-mapreduce/
cp native/target/usr/local/lib/libnativetask.so /usr/lib/hadoop/lib/native/
</pre></div>
</li>
<li>
<p>run MapReduce Pi example with native output collector</p>

<div class="highlight highlight-bash"><pre>hadoop jar hadoop-mapreduce-examples.jar pi -Dmapreduce.job.map.output.collector.class<span class="o">=</span>org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator 10 10
</pre></div>
</li>
<li>
<p>check the task log and NativeTask is successfully enabled if you see the following log</p>

<div class="highlight highlight-bash"><pre>INFO org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator: Native output collector can be successfully enabled! 
</pre></div>
</li>
</ol><p>Please check wiki for how to run MRv1 over NativeTask and HBase, Hive, Pig and Mahout support</p>

<h2>
<a name="user-content-contacts" class="anchor" href="#contacts"><span class="octicon octicon-link"></span></a>Contacts</h2>

<p>For questions and support, please contact </p>

<ul class="task-list">
<li>
<a href="https://github.com/clockfly">Sean Zhong</a> (<a href="mailto:xiang.zhong@intel.com">xiang.zhong@intel.com</a>)</li>
<li>
<a href="https://github.com/manuzhang">Manu Zhang</a> (<a href="mailto:tianlun.zhang@intel.com">tianlun.zhang@intel.com</a>)</li>
<li>
<a href="https://github.com/whjiang">Jiang Weihua</a> (<a href="mailto:weihua.jiang@intel.com">weihua.jiang@intel.com</a>)</li>
</ul><h2>
<a name="user-content-contributors" class="anchor" href="#contributors"><span class="octicon octicon-link"></span></a>Contributors</h2>

<ul class="task-list">
<li>
<a href="https://github.com/decster">Binglin Chang</a><br>
</li>
<li>
<a href="https://github.com/GarfiedYang">Yang Dong</a><br>
</li>
<li>
<a href="https://github.com/clockfly">Sean Zhong</a><br>
</li>
<li>
<a href="https://github.com/manuzhang">Manu Zhang</a><br>
</li>
<li>
<a href="https://github.com/zoken">Zhongliang Zhu</a><br>
</li>
<li>
<a href="https://github.com/huafengw">Vincent Wang</a><br>
</li>
<li><a href="https://github.com/sproblvem">Yan Dong</a></li>
<li>Fangqin Dai</li>
<li>Xusen Yin</li>
<li>Cheng Lian</li>
<li>
<a href="https://github.com/whjiang">Jiang Weihua</a> </li>
<li>Gansha Wu</li>
</ul><h2>
<a name="user-content-further-information" class="anchor" href="#further-information"><span class="octicon octicon-link"></span></a>Further information</h2>

<p>For further documents, please check the Wiki Page.</p></article>
  </div>

  </div>
</div>

<a href="#jump-to-line" rel="facebox[.linejump]" data-hotkey="l" class="js-jump-to-line" style="display:none">Jump to Line</a>
<div id="jump-to-line" style="display:none">
  <form accept-charset="UTF-8" class="js-jump-to-line-form">
    <input class="linejump-input js-jump-to-line-field" type="text" placeholder="Jump to line&hellip;" autofocus>
    <button type="submit" class="button">Go</button>
  </form>
</div>

        </div>

      </div><!-- /.repo-container -->
      <div class="modal-backdrop"></div>
    </div><!-- /.container -->
  </div><!-- /.site -->


    </div><!-- /.wrapper -->

      <div class="container">
  <div class="site-footer">
    <ul class="site-footer-links right">
      <li><a href="https://status.github.com/">Status</a></li>
      <li><a href="http://developer.github.com">API</a></li>
      <li><a href="http://training.github.com">Training</a></li>
      <li><a href="http://shop.github.com">Shop</a></li>
      <li><a href="/blog">Blog</a></li>
      <li><a href="/about">About</a></li>

    </ul>

    <a href="/">
      <span class="mega-octicon octicon-mark-github" title="GitHub"></span>
    </a>

    <ul class="site-footer-links">
      <li>&copy; 2014 <span title="0.07541s from github-fe129-cp1-prd.iad.github.net">GitHub</span>, Inc.</li>
        <li><a href="/site/terms">Terms</a></li>
        <li><a href="/site/privacy">Privacy</a></li>
        <li><a href="/security">Security</a></li>
        <li><a href="/contact">Contact</a></li>
    </ul>
  </div><!-- /.site-footer -->
</div><!-- /.container -->


    <div class="fullscreen-overlay js-fullscreen-overlay" id="fullscreen_overlay">
  <div class="fullscreen-container js-fullscreen-container">
    <div class="textarea-wrap">
      <textarea name="fullscreen-contents" id="fullscreen-contents" class="fullscreen-contents js-fullscreen-contents" placeholder="" data-suggester="fullscreen_suggester"></textarea>
    </div>
  </div>
  <div class="fullscreen-sidebar">
    <a href="#" class="exit-fullscreen js-exit-fullscreen tooltipped tooltipped-w" aria-label="Exit Zen Mode">
      <span class="mega-octicon octicon-screen-normal"></span>
    </a>
    <a href="#" class="theme-switcher js-theme-switcher tooltipped tooltipped-w"
      aria-label="Switch themes">
      <span class="octicon octicon-color-mode"></span>
    </a>
  </div>
</div>



    <div id="ajax-error-message" class="flash flash-error">
      <span class="octicon octicon-alert"></span>
      <a href="#" class="octicon octicon-remove-close close js-ajax-error-dismiss"></a>
      Something went wrong with that request. Please try again.
    </div>


      <script crossorigin="anonymous" src="https://assets-cdn.github.com/assets/frameworks-d556644b4638d7d08025b994fbb1b963da97b334.js" type="text/javascript"></script>
      <script async="async" crossorigin="anonymous" src="https://assets-cdn.github.com/assets/github-f8fc00b8934006933bc2391fd76f435ac85a7016.js" type="text/javascript"></script>
      
      
  </body>
</html>

