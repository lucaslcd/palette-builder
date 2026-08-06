import { AfterViewInit, Component, computed, effect, ElementRef, HostListener, inject, Signal, signal, ViewChild } from '@angular/core';
import { ColorState } from '../../services/color.state';
import { ColorPointer } from '../color-pointer/color-pointer';

export const COLOR_WHEEL_SATURATION = 0.6

@Component({
  selector: 'pbu-color-wheel',
  imports: [ColorPointer],
  templateUrl: './color-wheel.html',
  styleUrl: './color-wheel.css',
})
export class ColorWheel implements AfterViewInit {
  @ViewChild('colorWheel') canvasBinding!: ElementRef<HTMLCanvasElement>;
  private ctx!: CanvasRenderingContext2D | null

  colorState = inject(ColorState)
  hostElement = inject(ElementRef);

  stateForm = this.colorState.stateForm

  colors = computed(()=>this.colorState.state().colors)

  radius = signal<number>(0)
  cx = computed<number>(()=>this.radius())
  cy = computed<number>(()=>this.radius())


  constructor() {    
    effect(()=>{
      this.draw(COLOR_WHEEL_SATURATION)
    })
  }
  
  @HostListener('window:resize')
  onWindowResize(): void {
    this.resizeCanvas();
  }

  ngAfterViewInit(): void {
    const canvas = this.canvasBinding.nativeElement

    this.ctx = canvas.getContext('2d')

    this.resizeCanvas();
  }

  resizeCanvas() {
    const canvas = this.canvasBinding.nativeElement
    const container = this.hostElement.nativeElement
    
    canvas.width = container.clientWidth;
    canvas.height = container.clientHeight;
    
    this.radius.set(canvas.width / 2);

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
      
      gradient.addColorStop(1, `hsl(${angle}, ${Math.round(saturation * 100)}%, 50%)`);
      gradient.addColorStop(0, `hsl(${angle}, ${Math.round(saturation * 100)}%, 50%)`);

      this.ctx.fillStyle = gradient;
      this.ctx.fill();
    }
  }

}
