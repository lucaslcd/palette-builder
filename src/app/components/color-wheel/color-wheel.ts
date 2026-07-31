import { AfterViewInit, Component, effect, ElementRef, inject, Signal, signal, ViewChild } from '@angular/core';
import { StateService } from '../../services/state.service';

export const COLOR_WHEEL_SATURATION = 0.6

@Component({
  selector: 'pbu-color-wheel',
  imports: [],
  templateUrl: './color-wheel.html',
  styleUrl: './color-wheel.css',
})
export class ColorWheel implements AfterViewInit {
  @ViewChild('colorWheel') canvasBinding!: ElementRef<HTMLCanvasElement>;
  private ctx!: CanvasRenderingContext2D | null

  stateService = inject(StateService)

  radius = signal<number>(0)
  cx = signal<number>(0)
  cy = signal<number>(0)


  constructor() {    
    effect(()=>{
      this.draw(COLOR_WHEEL_SATURATION)
    })
  }


  ngAfterViewInit(): void {
    const canvas = this.canvasBinding.nativeElement

    this.ctx = canvas.getContext('2d')
    this.radius.set(canvas.width / 2);

    this.cx.set(this.radius());
    this.cy.set(this.radius());

    this.draw(COLOR_WHEEL_SATURATION)
  }


  draw(saturation: number) {
    if (this.ctx == null) {
      return;
    }

    for (let angle = 0; angle < 360; angle++) {
      const startAngle = (angle - 1) * Math.PI / 180;
      const endAngle = (angle + 1) * Math.PI / 180; // Overlap slightly to prevent gaps

      this.ctx.beginPath();
      this.ctx.moveTo(this.cx(), this.cy());
      this.ctx.arc(this.cx(), this.cy(), this.radius(), startAngle, endAngle);
      this.ctx.closePath();

      // Create a gradient running from the center (white) to the edge (pure color)
      const gradient = this.ctx.createRadialGradient(this.cx(), this.cy(), 0, this.cx(), this.cy(), this.radius());

      console.log(`hsl(${angle}, ${Math.round(saturation * 100)}%, 0%)`)
      
      gradient.addColorStop(1, `hsl(${angle}, ${Math.round(saturation * 100)}%, 50%)`);
      gradient.addColorStop(0, `hsl(${angle}, ${Math.round(saturation * 100)}%, 50%)`);

      this.ctx.fillStyle = gradient;
      this.ctx.fill();
    }
  }
}
