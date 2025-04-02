/**
 * Safe initialization wrapper for TacticSense
 */
(function() {
  // Only run when DOM is fully loaded
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
  } else {
    initApp();
  }

  // Main initialization function
  function initApp() {
    console.log('[TacticSense] Initializing application safely...');
    
    // Safety wrapper for any function
    function safeCall(fn) {
      try {
        fn();
      } catch (error) {
        console.error('[TacticSense] Error in initialization:', error);
      }
    }

    // Wait a moment to ensure Angular has loaded components
    setTimeout(function() {
      safeCall(initScrollTop);
      safeCall(initPreloader);
      safeCall(initLibraries);
    }, 100);
  }

  // Initialize scroll top button
  function initScrollTop() {
    var scrollTop = document.querySelector('.scroll-top');
    // Skip if element doesn't exist
    if (!scrollTop) {
      console.log('[TacticSense] Scroll top element not found, skipping initialization');
      return;
    }
    
    // Safe event handler for scroll
    function handleScroll() {
      if (scrollTop) {
        if (window.scrollY > 100) {
          scrollTop.classList.add('active');
        } else {
          scrollTop.classList.remove('active');
        }
      }
    }

    // Initial check
    handleScroll();
    
    // Add event listeners
    window.addEventListener('scroll', handleScroll);
    
    // Add click handler
    scrollTop.addEventListener('click', function(e) {
      e.preventDefault();
      window.scrollTo({
        top: 0,
        behavior: 'smooth'
      });
    });
    
    console.log('[TacticSense] Scroll top initialized successfully');
  }

  // Initialize preloader
  function initPreloader() {
    var preloader = document.querySelector('#preloader');
    // Skip if element doesn't exist
    if (!preloader) {
      console.log('[TacticSense] Preloader element not found, skipping initialization');
      return;
    }
    
    // Handle preloader removal
    window.addEventListener('load', function() {
      setTimeout(function() {
        if (preloader && preloader.parentNode) {
          preloader.classList.add('loaded');
        }
      }, 1000);
      setTimeout(function() {
        if (preloader && preloader.parentNode) {
          preloader.parentNode.removeChild(preloader);
        }
      }, 2000);
    });
    
    console.log('[TacticSense] Preloader initialized successfully');
  }

  // Initialize external libraries
  function initLibraries() {
    // Only initialize if libraries are available
    
    // AOS library initialization
    if (typeof window.AOS !== 'undefined') {
      window.AOS.init({
        duration: 800,
        easing: 'ease-in-out',
        once: true,
        mirror: false
      });
      console.log('[TacticSense] AOS initialized successfully');
    } else {
      console.log('[TacticSense] AOS not available, skipping initialization');
    }
    
    // Add similar checks for other libraries if needed
  }
})();