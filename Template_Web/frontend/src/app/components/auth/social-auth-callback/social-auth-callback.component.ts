import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { AuthService } from 'src/app/services/auth.service'

@Component({
  selector: 'app-social-auth-callback',
  template: '<div class="text-center my-5"><div class="spinner-border" role="status"></div><p>Authenticating...</p></div>'
})
export class SocialAuthCallbackComponent implements OnInit {
  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private authService: AuthService
  ) {}

  ngOnInit(): void {
    // Get provider from route data
    const provider = this.route.snapshot.data['provider'];
    // Get code from URL query params
    this.route.queryParams.subscribe(params => {
      const code = params['code'];
      
      if (code) {
        this.authService.processAuthToken(provider, code).subscribe({
          next: () => this.router.navigate(['/dashboard']),
          error: () => this.router.navigate(['/login'])
        });
      } else {
        this.router.navigate(['/login']);
      }
    });
  }
}